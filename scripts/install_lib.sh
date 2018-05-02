apt-get update
apt-get install -y cmake g++ gcc zlib1g-dev libssl-dev libcurl4-openssl-dev 
apt install expect-dev -y

#cd /root/aws-sdk-cpp/build_c4_8xlarge
#cmake -DCMAKE_BUILD_TYPE=Release -DBUILD_ONLY="s3;core" ..
#make -j16
#make install
#echo "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib/" >> ~/.bashrc

apt-get install libboost-all-dev -y
apt-get install libtbb-dev -y

apt install python-pip -y
pip install posix_ipc
pip install boto3
pip install numpy
pip install python-memcached
pip install cloudpickle
pip install pyyaml
pip install enum
pip install glob2
pip install tblib
pip install smart_open

apt install maven -y

mkdir /dev/mqueue; mount -t mqueue none /dev/mqueue

ulimit -q 10000000 
