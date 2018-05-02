sudo apt-get install -y gcc g++

mkdir /tmp/tf_install
cd /tmp/tf_install

#add-apt-repository ppa:openjdk-r/ppa -y
#apt-get update
#apt-get install -y openjdk-7-jdk
#export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64/
#export PATH=$PATH:$JAVA_HOME/bin
#wget https://github.com/bazelbuild/bazel/releases/download/0.4.3/bazel-0.4.3-jdk7-installer-linux-x86_64.sh
#./bazel-0.4.3-jdk7-installer-linux-x86_64.sh --user

echo "deb [arch=amd64] http://storage.googleapis.com/bazel-apt stable jdk1.8" | sudo tee /etc/apt/sources.list.d/bazel.list
curl https://bazel.build/bazel-release.pub.gpg | sudo apt-key add -
apt-get update
apt-get install bazel
