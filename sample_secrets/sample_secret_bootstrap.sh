#! /bin/bash

cd ~

echo '# secrets:' >> .bashrc
echo "export POSTGRES_PASSWORD='YOUR PASSWORD HERE'" >> .bashrc
echo "export POSTGRES_USER='YOUR POSTGRES USERNAME HERE'" >> .bashrc

echo "done secret bootstrap" > bootstrap.txt
echo `date` >> bootstrap.txt
