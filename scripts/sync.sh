DIR="$(pwd)/$(dirname $0)"
echo $DIR

if [ "$(hostname)" != "$(cat $DIR/../conf/coordinator)" ] && [ "$(ifconfig eth0| sed -En 's/127.0.0.1//;s/.*inet (addr:)?(([0-9]*\.){3}[0-9]*).*/\2/p')" != "$(cat $DIR/../conf/coordinator)" ]
then
  echo "Must be master"
  exit 1
fi
if [ "$1" == "" ]
then
  echo "sync.sh <path>"
  exit 1
fi
command -v realpath 2>&1 >/dev/null || sudo apt install realpath
path="$(realpath $1)"
for s in $(cat $DIR/../conf/agent)
do
  echo "Sync $path worker $s"
  rsync -ah $path $s:$(dirname $path) --delete
done
