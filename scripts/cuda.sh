#wget http://developer2.download.nvidia.com/compute/cuda/8.0/secure/Prod2/local_installers/cuda-repo-ubuntu1404-8-0-local-ga2_8.0.61-1_amd64.deb?pZ0GuYSWTNkypm9ST-EbnOz4OgS_Tnyx28QCZ3W1Do5JjvFbbigMtXWYv1_HkrrmkUaQaACcZw5VkyGZeLWdEC9gKrEIcK8-Rxcd1JU-r-Da9oLZ2VK9UE_7n_wip08hn0gaHB7x4574oE16XYVsYqTrWRKsy83mAu7Wx2ACuD3uLVVEeGhMKfjRiIRBeqOYhMYJrF_EQtIvtZqe-l72JtHoIQ -o cuda.deb
dpkg -i cuda.deb
sudo apt-get update
sudo apt-get upgrade -y
sudo apt-get install -y opencl-headers build-essential protobuf-compiler     libprotoc-dev libboost-all-dev libleveldb-dev hdf5-tools libhdf5-serial-dev     libopencv-core-dev  libopencv-highgui-dev libsnappy-dev libsnappy1     libatlas-base-dev cmake libstdc++6-4.8-dbg libgoogle-glog0 libgoogle-glog-dev     libgflags-dev liblmdb-dev git python-pip gfortran
apt-get install -y linux-image-extra-`uname -r` linux-headers-`uname -r` linux-image-`uname -r`
apt-get install -y cuda

apt-get install -y libopencv-dev python-opencv

#export PATH=$PATH:/usr/local/cuda-8.0/bin/
