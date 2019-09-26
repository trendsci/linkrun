#! /bin/bash

sudo yum install -y git
sudo python3 -m pip install ujson
sudo python3 -m pip install tldextract
sudo python3 -m pip install boto3
#sudo python3 -m pip install --upgrade requests


#export PYSPARK_PYTHON=python3

cd ~

echo '# LinkRun definitions below:' >> .bashrc
echo 'export PYSPARK_PYTHON=python3' >> .bashrc
echo 'git clone https://github.com/trendsci/linkrun.git' > gitclone.sh
chmod +x gitclone.sh

echo "done bootstrap" > bootstrap.txt
echo `date` >> bootstrap.txt
