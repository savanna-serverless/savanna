if [ "$(whoami)" != "root" ]
then
  echo "Must be root"
  exit 1
fi

DIR="$(pwd)/$(dirname $0)"


#kill python
for s in $(cat $DIR/../conf/agent)
do
  ssh -o StrictHostKeyChecking=no $s "killall -9 python &> /dev/null" &
done
ssh -o StrictHostKeyChecking=no $(cat $DIR/../conf/coordinator) "killall -9 python &> /dev/null" &
echo "waiting for killall python"
wait
echo done

#kill cache
for s in $(cat $DIR/../conf/agent)
do
  ssh $s "killall -9 cacheserver; rm -rf /dev/shm/cache/; rm -rf /tmp/cache;" &
done
echo "waiting for kill cacheserver and remove cache"
wait
echo "done"
killall -9 master


echo "Starting master $(cat $DIR/../conf/coordinator)"
ssh -f -o StrictHostKeyChecking=no $(cat $DIR/../conf/coordinator) "cd $DIR/../src/; ulimit -n 65535; nohup unbuffer ./master 2>&1 > /tmp/master &"

for s in $(cat $DIR/../conf/agent)
do
#  echo "Starting worker $s"
  ssh -f -o StrictHostKeyChecking=no $s "cd $DIR/../src/; ulimit -n 65535; ulimit -q 100000000; nohup unbuffer ./cacheserver $(cat $DIR/../conf/coordinator) 2>&1 > /tmp/cacheserver &"
done

