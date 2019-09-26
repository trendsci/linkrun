#! /usr/bin/python3
"""
This script automatically runs the LinkRun data pipeline
for the months specified in the config.json file.
"""

import ujson
import subprocess

def main():
    print("Started")
    spark_job_command = """$SPARK_HOME/bin/spark-submit \
        --packages  org.postgresql:postgresql:9.4.1207.jre7,org.apache.hadoop:hadoop-aws:2.7.0 \
        src/spark/read_wat_spark.py --testing_wat 0 --write_to_db 1 \
        --db_table temp2_08_2019  --verbose_output_rows 10 \
        --wat_paths_file_s3bucket commoncrawl \
        --wat_paths_file_s3key crawl-data/CC-MAIN-2019-35/wat.paths.gz \
        --first_wat_file_number 1 --last_wat_file_number 1"""

    #spark_job_command = """echo 'hi'\n echo 'bye'\n sleep 5 """
    subprocess.run(spark_job_command, shell=True,
        executable='/bin/bash')

    spark_job_command = """$SPARK_HOME/bin/spark-submit \
        --packages  org.postgresql:postgresql:9.4.1207.jre7,org.apache.hadoop:hadoop-aws:2.7.0 \
        src/spark/read_wat_spark.py --testing_wat 0 --write_to_db 1 \
        --db_table temp2_07_2019  --verbose_output_rows 10 \
        --wat_paths_file_s3bucket commoncrawl \
        --wat_paths_file_s3key crawl-data/CC-MAIN-2019-30/wat.paths.gz \
        --first_wat_file_number 1 --last_wat_file_number 1"""

    #spark_job_command = """echo 'hi'\n echo 'bye'\n sleep 5 """
    subprocess.run(spark_job_command, shell=True,
        executable='/bin/bash')


    print("Finished")


if __name__ == "__main__":
    main()
