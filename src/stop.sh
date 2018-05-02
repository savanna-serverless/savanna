if [ "$(whoami)" != "root" ]
then
  echo "Must be root"
  exit 1
fi


for s in $(cat /home/ubuntu/conf/workers)
do
  ssh $s "killall -9 cacheserver"
done

ssh $(cat /home/ubuntu/conf/master) "killall master"

