#! /bin/bash

start_time=`date -u +%s`


$SPARK_HOME/bin/spark-submit --packages  org.postgresql:postgresql:9.4.1207.jre7,org.apache.hadoop:hadoop-aws:2.7.0 ./src/spark/read_wat_spark.py


end_time=`date -u +%s`
elapsed=$((end_time-start_time))
printf "\n\n===============================\n"
printf "Script run time: ${elapsed}s\n\n"

