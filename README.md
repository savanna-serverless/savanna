# Savanna - An Architecutre Extension for Serverless Computing

Savanna is an extension that provides snapshot isolation, fault tolerance, and high throughput for serverless computing frameworks.
Our evaluation shows that Savanna improves serverless computing job performance by 1.4x to 6.4x.
Savanna is currently integrated with OpenWhisk and PyWren.

## AWS Deployment ##
We provide an automatic deployment script on AWS. You need an AWS account in order to run this script.

#### Step 1: Setup your AWS Credentials on your local machine ####
First, obtain your AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY from IAM, and put them in your environment varialbe.
If you have already done this, skip this step.
```
export AWS_ACCESS_KEY_ID=XXXXXXXXXXXXXXXXXXXX
export AWS_SECRET_ACCESS_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```
You can put this in your local .bashrc file so that you don't need to config it everytime.

#### Step 2: Obtain the source code and set config file ####
Git clone this repo to your local machine, and create the file ec2/launch.conf, you can use the ec2/launch.conf.template as example.
Following is an example launch.conf, please edit as you need
```
[AWS]
ami = ami-b7fde6cc # The target AMI, please keep it as is, as it contains necessary packages for Savanna
region = us-east-1 # AWS region
zone = us-east-1e # AWS zone
spot_price = 1.0 # You spot price bidding, if this value is <= 0, the regular instance will be used
aws_key = f99vm # The EC2 key pair name, it is necessary for your local machine to SSH to the launched EC2 instance.
                # You can import your local machine's public key ~/.ssh/id_ras.pub in the "Key Pairs" section in 
                # AWS EC2 Management Console(https://console.aws.amazon.com/ec2/v2/home?region=us-east-1#KeyPairs:sort=keyName)
aws_key_file = None # The path to private key to ssh to AWS servers. Set it to None if you want to use your default key (e.g. ~/.ssh/id_rsa)
instance_type = r4.large # The instance type
disk_size = 80 # Disk size, please make this >= than 80
placement_group = None #Placement Group

[Cluster]
cluster_name = savanna  #Cluster Name
num_workers = 1 #Number of machines in ths cluster


[Dev]
custom_vpc = False # Use a custom VPC, you can put a VPC-ID if you have an existing VPC to use. If set to false, a new VPC will be created
mount_nfs = False # For development use only, please keep this false

```

#### Step 3: Launch the cluster ####
On your local machine, run
```
cd ec2
./launch.sh
```
to launch a cluster on EC2.
You need fabric and boto3 to run the script, if you don't have them, use `pip install fabric/boto3` to install them
In the middle of the run, there will be a prompt to ask you to setup a S3 bucket for running example code. 
```
To run Savanna example code, you need a S3 bucket. 
Type a S3 bucket name to use an existing bucket. 
Type enter to create a new bucket (savanna603) in region us-east-1 and use it. 
Type 'No' to not configure any S3 bucket:
```
We have some example code that need to write data to a S3 bucket. 

If you do not want to run the example code, type No.

If you have an existing bucket to use, type the bucket name.

Type enter if you want the script to create a new bucket savanna603 (We suggest you to do this if you run Savanna the first time)

At the end of the script, it will output your coordinator node's IP
```
Your master node IP is XX.XX.XX.XX
```


#### Step 4: Run a simple sort job on your serverless cluster ####
SSH to the coordinator node with the IP you get in the previous step
```
ssh root@XX.XX.XX.XX
```
If you are not using the default (~/.ssh/id_rsa) private key path, use the following command to login
```
ssh -i path_to_key root@XX.XX.XX.XX
```
After logging in, go to 
```
cd ~/savanna/frontend/
```
Run the reset.sh, which will start the Savanna Metadata Coordinator and the Savanna Agent on each worker machine.
You can run this script if you want to restart Savanna
```
./reset.sh
```
Next, run the input data generator of the sort workload
```
root@ip-10-0-0-37:~/savanna/frontend# python sort_example.py gen
Initialization finished
Submitted 8 tasks
Duration 61.6198010445
Finish job in 61.6204338074 seconds
```
Then, you can run a sort job with Savanna
```
root@ip-10-0-0-37:~/savanna/frontend# python sort_example.py c
Initialization finished
Submitted 4 tasks
Duration 4.75358986855
Submitted 4 tasks
Duration 3.18673491478
Finish job in 7.94106602669 seconds
```
To run a job without Savanna and simply use S3 as storage, run
```
root@ip-10-0-0-37:~/savanna/frontend# python sort_example.py s3
Initialization finished
Submitted 4 tasks
Duration 19.1122591496
Submitted 4 tasks
Duration 14.9510660172
Finish job in 34.0635828972 seconds
```
As we can see, without Savanna, the runtime of sort becomes much larger. 
You can edit the sort_example.py to run a larger sort on more machines

#### Step 5: Terminate the cluster ####
To terminate your cluster, go back to your local machine and run
```
cd ec2
./launch.sh terminate
```







