#! /usr/bin/python3
"""
This script automatically runs the LinkRun data pipeline
for the months specified in the config.json file.
"""

import ujson
import subprocess
import time
import argparse


def main():
    """ Main automation script for automating common crawl Spark job"""

    print("Started")

    parser = argparse.ArgumentParser(description='Automation module')
    parser.add_argument('--config_path',
                        type=str,
                        help='Path to JSON automation config file')
    parsed_args = parser.parse_args()
    config_path = parsed_args.config_path


    start_time = time.time()
    log_file_name = "./logs/linkrun_automation_log_{}.log".format(
        str(time.time()).split(".")[0])

    with open('./src/automation/config_test.json', 'r') as f:
        config_json = ujson.load(f)

    spark_submit_command = "$SPARK_HOME/bin/spark-submit"
    spark_job_python_source = "./src/spark/read_wat_spark.py"

    for job in config_json:
        verbose_output_rows = 10
        commoncrawl_directory_name = job['id']
        first_wat_file_number = job['first_wat_file_number']
        last_wat_file_number = job['last_wat_file_number']
        job_month = job['month']
        job_year = job['year']
        testing_wat = 0  # 0 = common crawl wats, 1 = small test wat file
        write_to_db = 1
        database_table_name = job['output_table']

        # Autogenerate table name if needed:
        if database_table_name == "auto":
            database_table_name = "linkrunprod1.linkrundb_" + \
            commoncrawl_directory_name[-2:] + \
            "_" + str(job_month) + "_" + str(job_year) + \
            "_first_last_" + str(first_wat_file_number) + \
            "_" + str(last_wat_file_number)
        else:
            database_table_name = "linkrunprod1." + database_table_name

        if job['linkrun_done'] == 0:
            spark_job_command = """{spark_submit_command} \
            {deploy_params} \
            --conf "spark.decommissioning.timeout.threshold=1800" \
            --packages org.postgresql:postgresql:9.4.1207.jre7,org.apache.hadoop:hadoop-aws:2.7.0 \
            {spark_job_python_source} \
            --testing_wat {testing_wat} \
            --write_to_db {write_to_db} \
            --db_table {database_table_name}  \
            --verbose_output_rows {verbose_output_rows} \
            --wat_paths_file_s3bucket commoncrawl \
            --wat_paths_file_s3key crawl-data/{commoncrawl_directory_name}/wat.paths.gz \
            --first_wat_file_number {first_wat_file_number} \
            --last_wat_file_number {last_wat_file_number} \
            --jdbc_url 'jdbc:postgresql://linkrundb.caf9edw1merh.us-west-2.rds.amazonaws.com:5432/linkrundb' \
            """.format(
                spark_submit_command=spark_submit_command,
                spark_job_python_source=spark_job_python_source,
                testing_wat=testing_wat,
                write_to_db=write_to_db,
                database_table_name=database_table_name,
                verbose_output_rows=verbose_output_rows,
                commoncrawl_directory_name=commoncrawl_directory_name,
                first_wat_file_number=first_wat_file_number,
                last_wat_file_number=last_wat_file_number,
                deploy_params = " "#"--deploy-mode client --master yarn"
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
                + str(job) + "\n\n"
            with open(log_file_name, 'a+') as f:
                f.write(log_output)

    end_time = time.time()
    run_time = end_time - start_time
    log_output = "Finished automation job.\n" \
        + "Time for entire automation run (seconds): " \
        + str(run_time) + "\n" + "="*10 + "\n\n"
    with open(log_file_name, 'a+') as f:
        f.write(log_output)

    print("Finished")


if __name__ == "__main__":
    main()
