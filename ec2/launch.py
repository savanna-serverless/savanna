"""
fabric file to help with launching EC2 t2 instances and
getting OpenWhisk support set up. Use:

fab -f launch.py launch

# wait until launch finishes, and you can try basic_setup with the instance openwhisk_0
fab -f launch.py -H openwhisk_0 basic_setup

# when you're done, terminate. This will terminate all machines!
fab -f launch.py terminate
fab -f launch.py vpc_cleanup

Took inspiration from:
https://aws.amazon.com/blogs/aws/new-p2-instance-type-for-amazon-ec2-up-to-16-gpus/

"""
from fabric.api import *
from fabric.contrib import project
from time import sleep
import boto3
import os
import time
import subprocess
import ConfigParser
import threading
import traceback
import random

conf = ConfigParser.ConfigParser()
conf.read("launch.conf")
tgt_ami = conf.get("AWS", "ami")
AWS_REGION = conf.get("AWS", "region")
AWS_AVAILABILITY_ZONE = conf.get("AWS", "zone")
SPOT_PRICE = conf.get("AWS", "spot_price")  # get this <= 0 to use regular instances
CUSTOM_VPC = conf.get("Dev", "custom_vpc").lower() == "true"
MOUNT_NFS = conf.get("Dev", "mount_nfs").lower() == "true"
DISK_SIZE = int(conf.get("AWS", "disk_size"))
PLACEMENT_GROUP = None if conf.get("AWS", "placement_group").lower() == "none" else conf.get("AWS", "placement_group")

#import ssh public key to AWS
my_aws_key = conf.get("AWS", "aws_key")
my_aws_key_file = conf.get("AWS", "aws_key_file")
cluster_name = "cluster_" + conf.get("Cluster", "cluster_name", "")
worker_base_name = my_aws_key+"_" + cluster_name + "_"
NUM_WORKERS=int(conf.get("Cluster", "num_workers"))


try:
  boto3_session = boto3.session.Session()
  access_key = boto3_session.get_credentials().access_key
  secret_key = boto3_session.get_credentials().secret_key
except Exception as e:
  print "You need to set your AWS access_key and secret_key. Run:\n export AWS_ACCESS_KEY_ID=XXXXXXXXXXXXXXXXXXXX\n export AWS_SECRET_ACCESS_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n"
  exit()
# number of machines dedicated to a task
num_registry=1
num_edge=1
num_controllers=1
num_kafka=1
num_consul_servers=1
num_invokers=int(conf.get("Cluster", "num_workers"))
num_db=1


all_instance_names = [worker_base_name + str(x) for x in range(NUM_WORKERS)]

CONDA_DIR = "$HOME/anaconda"
WORKER_TYPE = conf.get("AWS", "instance_type")
# WORKER_TYPE = 'm4.16xlarge'    # for test case

def tags_to_dict(d):
  return {a['Key'] : a['Value'] for a in d}

#Roles and hosts should have the same names
def get_target_instance():
  role_to_host = {}
  ec2 = boto3.resource('ec2', region_name=AWS_REGION)

  host_list = []
  for i in ec2.instances.all():
    if i.state['Name'] == 'running':
      d = tags_to_dict(i.tags)
      if cluster_name in d['Name']:
        if d['Name'] in env.hosts:
          role_to_host[d['Name']] = 'ubuntu@{}'.format(i.public_dns_name)
          host_list.append('ubuntu@{}'.format(i.public_dns_name))
        elif len(env.hosts) == 0:
          role_to_host[d['Name']] = 'ubuntu@{}'.format(i.public_dns_name)
          host_list.append('ubuntu@{}'.format(i.public_dns_name))
  env.hosts.extend(host_list)
  print "found", role_to_host
  return role_to_host

env.disable_known_hosts = True
env.warn_only = True
env.roledefs.update(get_target_instance())
if my_aws_key_file == "" or my_aws_key_file.lower() == "none":
  print "Using Default Private Key file"
else:
  print "Using %s" % my_aws_key_file
  env.key_filename = my_aws_key_file
print "env.roles", env.roles
print "env.hosts", env.hosts


@task
def get_active_instances():
  ec2 = boto3.resource('ec2', region_name=AWS_REGION)

  for i in ec2.instances.all():
    if i.state['Name'] == 'running':
      d = tags_to_dict(i.tags)
      print "d[Name]", d['Name']

@task
def vpc_cleanup():
  ec2_resource = boto3.resource('ec2', region_name=AWS_REGION)
  ec2_client = boto3.client('ec2', region_name=AWS_REGION)

  #Delete security groups
  for security_group in ec2_resource.security_groups.all():
    print 'Security Group {}'.format(security_group.group_id)
    try:
      ec2_client.delete_security_group(GroupId=security_group.group_id)
      print '{} deleted'.format(security_group.group_id)
    except:
      print '{} not deleted'.format(security_group.group_id)
  #Delete Subnets
  for subnet in ec2_resource.subnets.all():
    print 'Subnet ID {}'.format(subnet.id)
    try:
      ec2_client.delete_subnet(SubnetId=subnet.id)
      print '{} deleted'.format(subnet.id)
    except:
      print '{} not deleted'.format(subnet.id)

  #Detach Gateways
  for vpc in ec2_resource.vpcs.all():
    for gateway in ec2_resource.internet_gateways.all():
      try:
        ec2_client.detach_internet_gateway(InternetGatewayId=gateway.id, VpcId=vpc.id)
        print '{} detached'.format(gateway.id)
      except:
        print '{} not detached'.format(gateway.id)

  #Delete Gateways
  for gateway in ec2_resource.internet_gateways.all():
    try:
      ec2_client.delete_internet_gateway(InternetGatewayId=gateway.id)
      print '{} deleted'.format(gateway.id)
    except:
      print '{} not deleted'.format(gateway.id)

  #Release IP Address
  for address in ec2_client.describe_addresses()['Addresses']:
    try:
      allocation_id = address['AllocationId']
      ec2_client.release_address(AllocationId=allocation_id)
      print '{} deleted'.format(str(address))
    except:
      print '{} not deleted'.format(str(address))

  #Delete Router Table
  for route_table in ec2_resource.route_tables.all():
    try:
      ec2_client.delete_route_table(RouteTableId=route_table.id)
      print '{} deleted'.format(route_table.id)
    except:
      print '{} not deleted'.format(route_table.id)
      
  #Finally, Delete VPC
  for vpc in ec2_resource.vpcs.all():
    try:
      ec2_client.delete_vpc(VpcId=vpc.id)
      print '{} deleted'.format(vpc.id)
    except:
      print '{} not deleted'.format(vpc.id)

def setup_network():
  use_dry_run = False
  ec2_resource = boto3.resource('ec2', region_name=AWS_REGION)
  ec2_client = boto3.client('ec2', region_name=AWS_REGION)
  
  #Create a VPC
  vpc = ec2_resource.create_vpc(DryRun=use_dry_run, CidrBlock='10.0.0.0/24')
  ec2_client.enable_vpc_classic_link(VpcId=vpc.vpc_id)
  ec2_client.modify_vpc_attribute(VpcId=vpc.vpc_id, EnableDnsSupport={'Value':True})
  ec2_client.modify_vpc_attribute(VpcId=vpc.vpc_id, EnableDnsHostnames={'Value':True})
  
  #Create an EC2 Security Group
  #security_group = ec2_client.create_security_group(GroupName='lambda_group', Description='Allow_all_ingress_egress', VpcId=vpc.vpc_id)
  desc_resp = ec2_client.describe_security_groups(Filters=[{'Name':'vpc-id', 'Values': [vpc.vpc_id] }])
  security_group = desc_resp['SecurityGroups'][0]
  group_id = security_group['GroupId']
  ip_permissions = [
    { 'IpProtocol': 'tcp',
      'FromPort': 22,
      'ToPort': 22,
      'IpRanges': [{'CidrIp': '0.0.0.0/0'}]
    }, 
    { 'IpProtocol': 'tcp',
      'FromPort': 22,
      'ToPort': 22,
      "Ipv6Ranges": [{'CidrIpv6': '::/0'}]
    }, 
#    { 'IpProtocol': '-1',
#      'FromPort': -1,
#      'ToPort': -1,
#      'UserIdGroupPairs': [{'GroupId': group_id}]
#    } 

  ]
#    ec2_client.authorize_security_group_egress(GroupId=group_id, IpPermissions=ip_permissions)
  ec2_client.authorize_security_group_ingress(GroupId=group_id, IpPermissions=ip_permissions) 
  #Create the subnet for the VPC
  subnet = vpc.create_subnet(DryRun=use_dry_run, CidrBlock='10.0.0.0/25', AvailabilityZone=AWS_AVAILABILITY_ZONE)
  ec2_client.modify_subnet_attribute(SubnetId=subnet.subnet_id, MapPublicIpOnLaunch={'Value':True})
  #Create an Internet Gateway
  gateway = ec2_resource.create_internet_gateway(DryRun=use_dry_run)
  gateway.attach_to_vpc(DryRun=use_dry_run, VpcId=vpc.vpc_id)
  #Create a Route table and add the route
  route_table = ec2_client.create_route_table(DryRun=use_dry_run, VpcId=vpc.vpc_id)
  route_table_id = route_table['RouteTable']['RouteTableId']
  ec2_client.associate_route_table(SubnetId=subnet.subnet_id, RouteTableId=route_table_id)
  ec2_client.create_route(DryRun=use_dry_run, DestinationCidrBlock='0.0.0.0/0',RouteTableId=route_table_id,GatewayId=gateway.internet_gateway_id)

  #create s3 vpc endpoint
  ec2_client.create_vpc_endpoint(ServiceName="com.amazonaws.%s.s3" % AWS_REGION, VpcId=vpc.vpc_id)

  return vpc, subnet, security_group

#Boot Reserved Instance
def setup_instance(ec2_client, ec2_resource, instance_names, instance_type, vpc, subnet, security_group, instance_count, volume_size):
  use_dry_run = False

  BlockDeviceMappings=[
    {
      'DeviceName': '/dev/sda1',
      'Ebs': {
        'VolumeSize': DISK_SIZE,
        'DeleteOnTermination': True,
        'VolumeType': 'standard',
      },
    },
  ]
  
  #Create a cluster of instances with specified type

  if float(SPOT_PRICE) > 0:
    net_interface = [{'DeviceIndex':0, 'SubnetId':subnet.subnet_id}]
    launch_spec = {'EbsOptimized':False,
            'ImageId':tgt_ami, 
            'BlockDeviceMappings':BlockDeviceMappings, 
            'KeyName':my_aws_key, 
            'NetworkInterfaces':net_interface, 
            #'SecurityGroupIds':[security_group['GroupId']], 
            'InstanceType':instance_type
            }
    spot_request = ec2_client.request_spot_instances(SpotPrice = SPOT_PRICE, Type = "one-time", InstanceCount=instance_count, LaunchSpecification=launch_spec)
    my_req_ids = [req["SpotInstanceRequestId"] for req in spot_request["SpotInstanceRequests"]]
    print "my_req_ids", my_req_ids
    try:
      while True:
        time.sleep(10)
        req_state = ec2_client.describe_spot_instance_requests(SpotInstanceRequestIds=my_req_ids)
        #print req_state
        instances = []
        for s in req_state['SpotInstanceRequests']:
          if s["SpotInstanceRequestId"] in my_req_ids:
            if s["State"] == "active":
              instance = ec2_resource.Instance(s['InstanceId'])
              if instance is not None and instance.state['Name'] == 'running':
                instances.append(instance)
            elif s["State"] == "failed":
              print str(s)
        if len(instances) == len(my_req_ids):
          print "All %d machines granted" % len(instances)
          instances_id = [ r['InstanceId'] for r in req_state['SpotInstanceRequests']]
          print "instances_id", instances_id
          break
        else:
          print "%d of %d machines granted, waiting longer" % (len(instances), len(my_req_ids))
    except Exception as e:
      traceback.print_exc() 
      print "Canceling spot instance requests"
      ec2_client.cancel_spot_instance_requests(SpotInstanceRequestIds=my_req_ids)
      exit()
  else:
    instances = ec2_resource.create_instances(ImageId=tgt_ami, MinCount=instance_count, MaxCount=instance_count, KeyName=my_aws_key, InstanceType=instance_type, SubnetId=subnet.subnet_id, BlockDeviceMappings = BlockDeviceMappings, EbsOptimized=False)

  for i in range(len(instances)):
    inst = instances[i]
    inst.wait_until_running()
    inst.reload()
    inst.create_tags(
      Resources=[
        inst.instance_id
    ],
      Tags=[
        {
          'Key': 'Name',
          'Value': instance_names[i]
        },
      ]
    )
  return instances

def sshable(server):
  try:
    if my_aws_key_file == "" or my_aws_key_file.lower() == "none":
      return "hello_world" in subprocess.check_output(["ssh", "-o", "StrictHostKeyChecking=no", "ubuntu@%s" % server,  "echo", "hello_world"])
    else:
      return "hello_world" in subprocess.check_output(["ssh", "-o", "StrictHostKeyChecking=no", "-i", my_aws_key_file, "ubuntu@%s" % server,  "echo", "hello_world"])

  except:
    return False

def wait_sshable(server):
  while not sshable(server):
    time.sleep(15)
  print server, "is sshable"

@task
@runs_once
def launch():
  #For Debugging
  use_dry_run = False
  
  ec2_resource = boto3.resource('ec2', region_name=AWS_REGION)
  ec2_client = boto3.client('ec2', region_name=AWS_REGION)
  if CUSTOM_VPC:
    subnet_map = {"us-east-1a":"subnet-6a43b240", "us-east-1d":"subnet-885260ed", "us-east-1e":"subnet-e5eaefd9", "us-east-1f":"subnet-3cc6a330"}
    vpc = ec2_resource.Vpc("vpc-5848343c")
    subnet = ec2_resource.Subnet(subnet_map[AWS_AVAILABILITY_ZONE])
    security_group = ec2_resource.SecurityGroup("sg-995b2ee0")
  else:
    vpc, subnet, security_group = setup_network()
  all_worker_ips = []

  #Launch OpenWhisk workers + bootstrapper
  inst_names = []
  for instance_num in range(NUM_WORKERS+1):
    if instance_num==0:
      inst_name = '{}{}'.format(my_aws_key, "_" + cluster_name + "_master")
    else:
      inst_name = '{}{}'.format(worker_base_name, instance_num)
    inst_names.append(inst_name)
  print inst_names
  instances = setup_instance(ec2_client, ec2_resource, inst_names, WORKER_TYPE, vpc, subnet, security_group, NUM_WORKERS+1, 200)
  for instance in instances:
    print 'lambda instance setup at {}'.format(instance.public_ip_address)
    all_worker_ips.append(instance)


  threads = [threading.Thread(target=wait_sshable, args=(i.public_ip_address,)) for i in instances]
  [ t.start() for t in threads]
  [ t.join() for t in threads]
 

@task
@runs_once
def terminate():
  ec2 = boto3.resource('ec2', region_name=AWS_REGION)

  insts = []
  for i in ec2.instances.all():
    print i, i.state['Name']
    if i.state['Name'] == 'running':
      d = tags_to_dict(i.tags)
      if cluster_name in d['Name']:
        i.terminate()
        insts.append(i)


@task
@parallel
def savanna_cluster_setup():
    run("sudo sh -c 'apt-get update; apt-get install git -y; apt-get install libtbb-dev -y; rm -rf /root/savanna; git clone https://github.com/savanna-serverless/savanna.git /root/savanna; cd /root/savanna/src; cmake .; cmake .; make -j8; echo \"export AWS_ACCESS_KEY_ID=%s\" >> /root/.bashrc; echo \"export AWS_SECRET_ACCESS_KEY=%s\" >> /root/.bashrc; cp /root/.bashrc /root/.profile ; mkdir -p /root/.ssh; cat /home/ubuntu/.ssh/authorized_keys > /root/.ssh/authorized_keys; ' &> /tmp/savanna_cluster_setup.log0" % (access_key, secret_key))

@task
@runs_once
def savanna_master_setup():
  ec2 = boto3.resource('ec2', region_name=AWS_REGION)
  env.user = "ubuntu"
  worker_hosts = []
  worker_private = []
  for i in ec2.instances.all():
    if i.state['Name'] == 'running':
      d = tags_to_dict(i.tags)
      print d['Name'], i.public_ip_address
      if d['Name'] == my_aws_key+ "_" + cluster_name + "_master":
        bootstrapper_host = i.public_ip_address
        bootstrapper_private = i.private_ip_address
      elif d['Name'].startswith(worker_base_name):
        worker_hosts.append(i.public_ip_address)
        worker_private.append(i.private_ip_address)

  env.host_string = bootstrapper_host
  run("sudo sh -c \"echo | ssh-keygen -P ''\"")
  public_key = run("sudo sh -c \"cat /root/.ssh/id_rsa.pub\"")

  # allow bootstrapper to access workers
  for h in worker_hosts + [bootstrapper_host]:
    env.host_string = h
    run("sudo sh -c \"echo '"+public_key+"' >> /root/.ssh/authorized_keys\"")

  env.host_string = bootstrapper_host  
  worker_str = "\n".join(worker_private)
  master_str = bootstrapper_private
  run("sudo sh -c \"echo '%s' > /root/savanna/conf/agent; echo '%s' > /root/savanna/conf/coordinator\"" % (worker_str, master_str))

  while True:
    bucket_name = "savanna%s" % random.randint(0, 1000)
    bucket_name_input = raw_input("To run Savanna example code, you need a S3 bucket. Type a S3 bucket name to use an existing bucket. Type enter to create a new bucket (%s) in region %s and use it. Type 'No' to not configure any S3 bucket:\n" % (bucket_name, AWS_REGION))
    if bucket_name_input.strip().lower() == "no":
      print "No S3 bucket configured"
      bucket_name = "None"
      break
    else:
      if bucket_name_input.strip() != "":
        bucket_name = bucket_name_input.strip()
        print "Using bucket %s" % bucket_name
      else:
        s3 = boto3.resource('s3')
        print "Creating bucket %s" % bucket_name
        try:
          if AWS_REGION == "us-east-1":
            s3.create_bucket(Bucket=bucket_name)
          else:
            s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration= {'LocationConstraint': AWS_REGION})
        except Exception as e:
          print "Failed to create bucket %s" % str(e)
          continue
      break
  run("sudo sh -c \"echo '[Savanna]' > /root/savanna/conf/conf; echo 's3_bucket = %s' >> /root/savanna/conf/conf\"" % bucket_name)
  print "\n\nYour master node IP is %s\n" % bootstrapper_host


@task
@parallel
def my_cluster_setup():
    run("sudo sh -c 'apt update; apt install -y nfs-common; apt-get install git -y; apt-get install libtbb-dev -y;'")
    run("sudo sh -c 'if mount | grep /root > /dev/null; then echo already mounted; else mv /root /root_bak; mkdir /root; mount -t nfs4 fs-717ac738.efs.us-east-1.amazonaws.com:/ /root; fi'")
    #run("sudo sh -c /root/serverless/scripts/install_lib.sh &> /tmp/install_lib.log")

@task
@runs_once
def my_master_setup():
  ec2 = boto3.resource('ec2', region_name=AWS_REGION)
  env.user = "ubuntu"
  worker_hosts = []
  worker_private = []
  for i in ec2.instances.all():
    if i.state['Name'] == 'running':
      d = tags_to_dict(i.tags)
      print d['Name'], i.public_ip_address
      if d['Name'] == my_aws_key+ "_" + cluster_name + "_master":
        bootstrapper_host = i.public_ip_address
        bootstrapper_private = i.private_ip_address
      elif d['Name'].startswith(worker_base_name):
        worker_hosts.append(i.public_ip_address)
        worker_private.append(i.private_ip_address)

  env.host_string = bootstrapper_host  
  worker_str = "\n".join(worker_private)
  master_str = bootstrapper_private
  run("sudo sh -c \"rm -rf /root/serverless/conf; mkdir -p /root/serverless/conf; echo '%s' > /root/serverless/conf/agent; echo '%s' > /root/serverless/conf/coordinator; echo '[Savanna]' > /root/serverless/conf/conf;  echo 's3_bucket = savannatest' >> /root/serverless/conf/conf; git config core.fileMode false\"" % (worker_str, master_str))



@task
@parallel
def whisk_cluster_setup():
  run("sudo apt-get update")
  run("sudo apt-get -y install git")
  run("sudo apt-get -y install python-pip")
  run("sudo apt-get htop")
  run("sudo apt-get -y install build-essential libssl-dev libffi-dev python-dev")
  run("sudo pip install cloudpickle")
  #run("sudo pip install numpy")
  run("sudo pip install boto3")
  run("sudo pip install posix_ipc")

  ## prereq for run_master
  run("sudo apt-get install -y cmake g++ gcc zlib1g-dev libssl-dev libcurl4-openssl-dev ")
  run("sudo apt install expect-dev -y")
  run("sudo apt-get install libboost-all-dev -y")
  run("sudo mkdir /dev/mqueue; sudo mount -t mqueue none /dev/mqueue")
  run('echo "export LD_LIBRARY_PATH=\$LD_LIBRARY_PATH:/usr/local/lib/" >> ~/.bashrc')

  ## install docker clean
  run("curl -s https://raw.githubusercontent.com/ZZROTDesign/docker-clean/v2.0.4/docker-clean | sudo tee /usr/local/bin/docker-clean > /dev/null && sudo chmod +x /usr/local/bin/docker-clean")


  run("sudo rm -rf openwhisk")
  run("git clone https://github.com/pxgao/openwhisk.git")
  with cd("~/openwhisk/tools/ubuntu-setup"):
    run("./all.sh")

  ## setup wsk cli
  run("echo 'alias wsk=/home/ubuntu/openwhisk/bin/wsk' >> ~/.bashrc && source ~/.bashrc")
  
  ## setup crontab to kill long-running warm JS container
  run("(crontab -l 2>/dev/null; echo '*/20 * * * * docker rm -f $(docker ps --format \"{{.Names}}\" | grep Js)') | crontab -")



@task
@runs_once
def whisk_master_setup():
  ec2 = boto3.resource('ec2', region_name=AWS_REGION)
  env.user = "ubuntu"
  worker_hosts = []
  worker_private = []
  for i in ec2.instances.all():
    if i.state['Name'] == 'running':
      d = tags_to_dict(i.tags)
      print d['Name'], i.public_ip_address
      if d['Name'] == my_aws_key+ "_" + cluster_name + "_master":
        bootstrapper_host = i.public_ip_address
        bootstrapper_private = i.private_ip_address
      elif d['Name'].startswith(worker_base_name):
        worker_hosts.append(i.public_ip_address)
        worker_private.append(i.private_ip_address)

  env.host_string = bootstrapper_host
  ssh_contents = run("ls ~/.ssh/")
  run("echo | ssh-keygen -P ''")
  public_key = run("cat ~/.ssh/id_rsa.pub")

  # allow bootstrapper to access workers
  for h in worker_hosts:
    env.host_string = h
    run("docker stop $(docker ps -a -q)")
    run("docker rm $(docker ps -a -q)")
    run("docker rmi $(docker images -q)")
    with cd("~/.ssh"):
      run("echo '"+public_key+"' >> authorized_keys")
      print run("cat authorized_keys")


  env.host_string = bootstrapper_host  
  ## may need to re-use some workers for multiple tasks if NUM_WORKERS is small
  total_wanted_num_wks = num_registry+num_edge+num_controllers+num_kafka+num_consul_servers+num_invokers+num_db
  if total_wanted_num_wks>NUM_WORKERS-1:
    worker_private_expand = worker_private*(total_wanted_num_wks/NUM_WORKERS+1)
    print "executed expand worker_private", worker_private
  else:
    worker_private_expand = worker_private
  i = 0
  registry_str = "\n".join(worker_private_expand[i:i+num_registry]); i+=num_registry
  edge_str = "\n".join(worker_private_expand[i:i+num_edge]); i+=num_edge
  controllers_str = "\n".join(worker_private_expand[i:i+num_controllers]); i+=num_controllers
  kafka_str = "\n".join(worker_private_expand[i:i+num_kafka]); i+=num_kafka
  consul_servers_str = "\n".join(worker_private_expand[i:i+num_consul_servers]); i+=num_consul_servers
  invokers_str = "\n".join(worker_private_expand[i:i+num_invokers]); i+=num_invokers
  db_str = "\n".join(worker_private_expand[i:i+num_db]); i+=num_db

  config_file_str = "ansible ansible_connection=local\n\
        [registry]\n\
        {}\n\
        [edge]\n\
        {}\n\
        [apigateway:children]\n\
        edge\n\
        [controllers]\n\
        {}\n\
        [kafka]\n\
        {}\n\
        [consul_servers]\n\
        {}\n\
        [invokers]\n\
        {}\n\
        [db]\n\
        {}".format(registry_str, edge_str, controllers_str, kafka_str, consul_servers_str, invokers_str, db_str)

  with cd("~/openwhisk/ansible"):
    with cd("environments/distributed"):
      run("echo '"+config_file_str+"' > hosts")
    run("ansible all -i environments/distributed -m ping")

  with cd("~/openwhisk/ansible"):
    run("ansible-playbook -i environments/distributed setup.yml")
    run("ansible-playbook -i environments/distributed prereq_build.yml")
    run("ansible-playbook -i environments/distributed registry.yml")

    run("cd .. && sudo ./gradlew distDocker -PdockerHost={}:4243 -PdockerRegistry={}:5000".format(registry_str, registry_str))
    
    run("ansible-playbook -i environments/distributed couchdb.yml")
    run("ansible-playbook -i environments/distributed initdb.yml")
    run("ansible-playbook -i environments/distributed wipe.yml")
    run("ansible-playbook -i environments/distributed openwhisk.yml")
    run("ansible-playbook -i environments/distributed postdeploy.yml")
    run("../bin/wsk property set --auth $(cat files/auth.whisk.system) --apihost {}".format(edge_str))
    run("../bin/wsk -i -v action invoke /whisk.system/samples/helloWorld --blocking --result")

  # allow dispatcher to ship Dockerfile to workers
  for h in worker_hosts:
    env.host_string = h
    Dockerfile_str = 'FROM {}:5000/whisk/python2action\n RUN pip install boto3 && pip install posix_ipc \n CMD ["/bin/bash", "-c", "cd pythonAction && python -u pythonrunner.py"]'.format(registry_str)
    run("mkdir ~/new-dir")
    with cd("~/new-dir"):
      run("echo '{}' > Dockerfile".format(Dockerfile_str))
      run("docker build -t {}:5000/whisk/python2action .".format(registry_str))
      ## used to identify where the lambda is
      run("hostname > /dev/shm/ip-address")

  ##mount NFS & setup conf folder
  worker_str = "\n".join(worker_private)
  master_str = bootstrapper_private
  for h in worker_hosts + [bootstrapper_host]:
    env.host_string = h
    if MOUNT_NFS:
      run("sudo apt install -y nfs-common; sudo umount /root; sudo mv -f /root /root_bak; sudo mkdir /root; sudo mount -t nfs4 fs-717ac738.efs.us-east-1.amazonaws.com:/ /root;")
    run("rm -rf /home/ubuntu/conf; mkdir -p /home/ubuntu/conf; echo '%s' > /home/ubuntu/conf/workers; echo '%s' > /home/ubuntu/conf/master; git config core.fileMode false" % (worker_str, master_str))










@task
@parallel
def whisk_single_setup():

  with cd("~/openwhisk"):
    run("sudo ./gradlew distDocker")

  API_host = "http://172.17.0.1:10001"

  with cd("~/openwhisk"):
    run("./bin/wsk property set --apihost " + API_host)
    run("./bin/wsk property set --auth `cat ansible/files/auth.guest`")
  ## clean any docker if exists
  run("docker-clean stop")

  ## deploy Ephemeral CouchDB using ansible
  run("sudo pip install ansible==2.3.0.0")
  with cd("~/openwhisk/ansible"):
    run("ansible-playbook setup.yml")
    run("ansible-playbook prereq.yml")
    run("ansible-playbook couchdb.yml")
    run("ansible-playbook initdb.yml")
    run("ansible-playbook wipe.yml")
    run("ansible-playbook openwhisk.yml")
    run("ansible-playbook postdeploy.yml")

  ## test if deployment succeeded
  with cd("~/openwhisk"):
    run("./bin/wsk action invoke /whisk.system/utils/echo -p message hello --result")

@task
@parallel
def clean_docker():
  run("docker stop $(docker ps -a -q)")
  run("docker rm $(docker ps -a -q)")
  run("docker rmi $(docker images -q)")


