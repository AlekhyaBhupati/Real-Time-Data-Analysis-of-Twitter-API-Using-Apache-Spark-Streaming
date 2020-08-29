yes "y" | sudo yum install kafka
##curl -O https://repo.anaconda.com/archive/Anaconda3-5.3.1-Linux-x86_64.sh
##bash Anaconda3-5.3.1-Linux-x86_64.sh -u
sudo yum update
sudo yum -y install python34
wget https://bootstrap.pypa.io/get-pip.py
sudo python3.4 get-pip.py
sudo cp /usr/lib/hive/conf/hive-site.xml /usr/lib/spark/conf/
sudo cp /etc/hive/conf.dist/hive-site.xml /etc/spark/conf/
wget https://www.python.org/ftp/python/2.7.16/Python-2.7.16.tgz
#sudo easy_install pip
tar xzf Python-2.7.16.tgz
cd Python-2.7.16
sudo ./configure --enable-optimizations
sudo make altinstall
cd -
echo "alias python=/usr/local/bin/python2.7" >> ~/.bashrc
echo "export PYSPARK_PYTHON=python2.7"  >> ~/.bashrc
source ~/.bashrc
python get-pip.py --user
sudo sed -i -e 's/#!/#!\/usr\/local\/bin\/python2.7/g' /usr/local/bin/pip2.7
