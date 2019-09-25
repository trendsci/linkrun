#! /bin/bash

start_time=`date -u +%s`


#$SPARK_HOME/bin/spark-submit --packages  org.postgresql:postgresql:9.4.1207.jre7,org.apache.hadoop:hadoop-aws:2.7.0 ./src/spark/read_wat_spark.py


$SPARK_HOME/bin/spark-submit --packages  org.postgresql:postgresql:9.4.1207.jre7,org.apache.hadoop:hadoop-aws:2.7.0 src/spark/read_wat_spark.py --testing_wat 0 --write_to_db 1 --db_table temp2  --verbose_output_rows 0 --wat_paths_file_s3bucket linkrun --wat_paths_file_s3key wat.paths --first_wat_file_number 0 --last_wat_file_number 0



end_time=`date -u +%s`
elapsed=$((end_time-start_time))
printf "\n\n===============================\n"
printf "Script run time: ${elapsed}s\n\n"
