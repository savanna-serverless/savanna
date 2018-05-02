if [ "$(whoami)" != "root" ]
then
  echo "Must be root"
  exit 1
fi

echo "Starting master $(cat /home/ubuntu/conf/master)"
ssh -f -o StrictHostKeyChecking=no $(cat /home/ubuntu/conf/master) "cd /root/serverless/cache/cacheserver/; nohup unbuffer ./master 2>&1 > /tmp/master &"

for s in $(cat /home/ubuntu/conf/workers)
do
  echo "Starting worker $s"
  ssh -f -o StrictHostKeyChecking=no $s "cd /root/serverless/cache/cacheserver/; ulimit -n 65535; ulimit -q 100000000; nohup unbuffer ./cacheserver $(cat /home/ubuntu/conf/master) 2>&1 > /tmp/cacheserver &"
done
