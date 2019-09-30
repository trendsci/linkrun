#! /usr/bin/python3
"""
This script automatically runs the LinkRun data pipeline
for the months specified in the config.json file.
"""

import ujson
import subprocess
import time


def main():
    print("Started")
    start_time = time.time()
    log_file_name = "./logs/linkrun_automation_log_{}.log".format(str(time.time()).split(".")[0])

    with open('./src/automation/config_test.json','r') as f:
        config_json = ujson.load(f)

    spark_submit_command = "$SPARK_HOME/bin/spark-submit"
    spark_job_python_source = "./src/spark/read_wat_spark.py"


    for job in config_json:
        database_table_name = job['output_table']
        verbose_output_rows = 10
        commoncrawl_directory_name = job['id']
        first_wat_file_number = job['first_wat_file_number']
        last_wat_file_number = job['last_wat_file_number']
        testing_wat = 1 # 0 = common crawl wats, 1 = small test wat file
        if job['linkrun_done'] == 0:
            spark_job_command = """{spark_submit_command} \
                --deploy-mode client --master yarn \
                --conf "spark.decommissioning.timeout.threshold=360" \
                --packages org.postgresql:postgresql:9.4.1207.jre7,org.apache.hadoop:hadoop-aws:2.7.0 \
                {spark_job_python_source} --testing_wat {testing_wat} --write_to_db 1 \
                --db_table {database_table_name}  \
                --verbose_output_rows {verbose_output_rows} \
                --wat_paths_file_s3bucket commoncrawl \
                --wat_paths_file_s3key crawl-data/{commoncrawl_directory_name}/wat.paths.gz \
                --first_wat_file_number {first_wat_file_number} \
                --last_wat_file_number {last_wat_file_number}""".format(
                spark_submit_command = spark_submit_command,
                spark_job_python_source = spark_job_python_source,
                testing_wat = testing_wat,
                database_table_name = database_table_name,
                verbose_output_rows = verbose_output_rows,
                commoncrawl_directory_name = commoncrawl_directory_name,
                first_wat_file_number = first_wat_file_number,
                last_wat_file_number = last_wat_file_number
                )
            subprocess.run(spark_job_command, shell=True,
                executable='/bin/bash')


            end_time = time.time()
            run_time = end_time - start_time


            log_output = "Job: \n" + "database_table_name" \
                + "commoncrawl_directory_name: " \
                + commoncrawl_directory_name + "\n" \
                + "database_table_name: " \
                + database_table_name + "\n" \
                + "first_wat_file_number: " \
                + str(first_wat_file_number) + "\n" \
                + "last_wat_file_number" \
                + str(last_wat_file_number) + "\n" \
                + "Total job run time (seconds): " + str(run_time)\
                + "\n\n"


            with open(log_file_name, 'a+') as f:
                f.write(log_output)

        # If job is already done, skip it and record the fact that it's skipped.
        if job['linkrun_done'] == 1:
            log_output = "Skipped job: \n" \
                + str(job) + + "\n\n"
            with open(log_file_name, 'a+') as f:
                f.write(log_output)

    end_time = time.time()
    run_time = end_time - start_time
    log_output = "Finished automation job.\n" \
        + "Time for entire automation run (seconds): " \
        + str(run_time) + "\n" + "="*10 + "\n\n"
    with open(log_file_name, 'a+') as f:
        f.write(log_output)

            # subprocess.run(
            #     """cd ~; echo '{}' >> {}""".format(
            #         log_output,log_file_name),
            #     shell=True,
            #     executable='/bin/bash')

    # spark_job_command = """$SPARK_HOME/bin/spark-submit \
    #     --packages org.postgresql:postgresql:9.4.1207.jre7,org.apache.hadoop:hadoop-aws:2.7.0 \
    #     src/spark/read_wat_spark.py --testing_wat 0 --write_to_db 1 \
    #     --db_table temp2_08_2019  --verbose_output_rows 10 \
    #     --wat_paths_file_s3bucket commoncrawl \
    #     --wat_paths_file_s3key crawl-data/CC-MAIN-2019-35/wat.paths.gz \
    #     --first_wat_file_number 1 --last_wat_file_number 1"""
    #
    # #spark_job_command = """echo 'hi'\n echo 'bye'\n sleep 5 """
    # subprocess.run(spark_job_command, shell=True,
    #     executable='/bin/bash')
    #
    # spark_job_command = """$SPARK_HOME/bin/spark-submit \
    #     --packages  org.postgresql:postgresql:9.4.1207.jre7,org.apache.hadoop:hadoop-aws:2.7.0 \
    #     src/spark/read_wat_spark.py --testing_wat 0 --write_to_db 1 \
    #     --db_table temp2_07_2019  --verbose_output_rows 10 \
    #     --wat_paths_file_s3bucket commoncrawl \
    #     --wat_paths_file_s3key crawl-data/CC-MAIN-2019-30/wat.paths.gz \
    #     --first_wat_file_number 1 --last_wat_file_number 1"""
    #
    # #spark_job_command = """echo 'hi'\n echo 'bye'\n sleep 5 """
    # subprocess.run(spark_job_command, shell=True,
    #     executable='/bin/bash')


    print("Finished")


if __name__ == "__main__":
    main()
