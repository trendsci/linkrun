#! /bin/bash

start_time=`date -u +%s`

# go to project directory
cd /home/sergey/projects/insight/mainproject/linkrun
printf "pwd: $(pwd)\n\n"

current_file='CC-MAIN-20190715175205-20190715200159-00000.warc.wat.gz'
cc_data_dir='cc_data'

# clean up any old files. to get prompt add "I" or "i"
test -d $cc_data_dir && rm -rv $cc_data_dir

printf ">Currently processed file:\n$current_file\n\n"
printf ">Creating directory for data:\n$(pwd)/$cc_data_dir\n\n"

#read -p "Continue? (y/n): " response && [[ $response == [y] ]] || exit 1

# create data directory if not exists
test -d $cc_data_dir || mkdir $cc_data_dir
cd $cc_data_dir

# get data
cp /home/sergey/projects/insight/mainproject/1/testwat/$current_file .

printf ">Data directory:\n"
ls

printf "\n"
#printf ">gunzipping:\n"
#gzip -d $current_file


end_time=`date -u +%s`
elapsed=$((end_time-start_time))
printf "Script run time: ${elapsed}s\n\n"
