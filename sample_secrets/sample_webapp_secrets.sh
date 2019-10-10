#! /bin/bash

cd ~

echo '# secrets:' >> .bashrc
echo "export POSTGRES_PASSWORD='YOUR PASSWORD HERE'" >> .bashrc
echo "export POSTGRES_USER='YOUR DATABASE USERNAME HERE'" >> .bashrc
echo "export POSTGRES_LOCATION='LOCATION OF DATABASE HERE'" >> .bashrc
