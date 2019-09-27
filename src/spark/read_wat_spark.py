#! /usr/bin/python3

## MIGHT NEED TO RUN THIS USING:
# spark-submit --packages org.apache.hadoop:hadoop-aws:2.7.0 ./path_to_this_python_code
# might need to run in shell:
# export PYSPARK_PYTHON=python3

# submit jobs using:
# $SPARK_HOME/bin/spark-submit --master spark://ip-10-0-0-11.us-west-2.compute.internal:7077  read_wat_spark.py
# current server: ec2-35-163-37-42.us-west-2.compute.amazonaws.com


# PORT forward to connect to db:
# ssh -o ServerAliveInterval=10 -i sergey-IAM-keypair.pem -N -L 10000:localhost:5432 ubuntu@54.70.95.199

## if missing packages, can run:
# $SPARK_HOME/bin/spark-submit --packages  org.postgresql:postgresql:9.4.1207.jre7,org.apache.hadoop:hadoop-aws:2.7.0 ./read_wat_spark.py


# submit in EMR cluter, check if correct:
# spark-submit --deploy-mode cluster --master yarn --packages org.postgresql:postgresql:9.4.1207.jre7,org.apache.hadoop:hadoop-aws:2.7.0 ./src/spark/read_wat_spark.py --wat_number 4 --write_to_db 0

# $SPARK_HOME/bin/spark-submit --packages  org.postgresql:postgresql:9.4.1207.jre7,org.apache.hadoop:hadoop-aws:2.7.0 src/spark/read_wat_spark.py --wat_number 0 --write_to_db 0 --db_table temp2  --verbose_output_rows 100


## bash execution command:
# $SPARK_HOME/bin/spark-submit --packages  org.postgresql:postgresql:9.4.1207.jre7,org.apache.hadoop:hadoop-aws:2.7.0 src/spark/read_wat_spark.py --testing_wat 0 --write_to_db 1 --db_table temp2  --verbose_output_rows 10 --wat_paths_file_s3bucket linkrun --wat_paths_file_s3key wat.paths --first_wat_file_number 0 --last_wat_file_number 0


#most recent submit script=
#spark-submit --deploy-mode client --master yarn --packages org.postgresql:postgresql:9.4.1207.jre7,org.apache.hadoop:hadoop-aws:2.7.0 src/spark/read_wat_spark.py --testing_wat 0 --write_to_db 1 --db_table temper_11  --verbose_output_rows 10 --wat_paths_file_s3bucket commoncrawl --wat_paths_file_s3key crawl-data/CC-MAIN-2019-35/wat.paths.gz --first_wat_file_number 1 --last_wat_file_number 3

# add to large jobs:
# --conf "spark.decommissioning.timeout.threshold=360"

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

import ujson as json
import tldextract as tldex
import time

import boto3
import argparse
import gzip
from io import BytesIO

def get_json(line):
    try:
        line = line.lower()
        json_data = json.loads(line)
        #print (line)
        return json_data
    except:
        pass


def get_json_uri(json_line):
    try:
        # Mixed caps (in original json):
        #current_uri = json_line["Envelope"]["WARC-Header-Metadata"]["WARC-Target-URI"]
        current_uri = json_line["envelope"]["warc-header-metadata"]["warc-target-uri"]
        #print("current uri: ",current_uri)
        return (current_uri, json_line)
    except Exception as e:
        pass
        #print("No Target URI")
        #print("error: ",e)

def parse_domain(uri):
    try:
        ##print("URI!! ",type(uri),"\n",uri)
        subdomain, domain, suffix = tldex.extract(uri)
        ##print("parsed!!: ",subdomain, domain, suffix)
        #return subdomain + "." + domain + "." + suffix
        return subdomain, domain, suffix
    except Exception as e:
        #print("error:",e)
        pass

def get_json_links(json_line):
    try:
        # Mixed caps (in original json):
        #links = json_line["Envelope"]["Payload-Metadata"]["HTTP-Response-Metadata"]["HTML-Metadata"]["Links"]
        links = json_line["envelope"]["payload-metadata"]["http-response-metadata"]["html-metadata"]["links"]
        return links
    except Exception as e:
        #print("error: ",e)
        pass

def filter_links(json_links, page_subdomain, page_domain, page_suffix):
    #print("filtering these links")
    #print("json_links: ",json_links, "\npage_subdomain: ", page_subdomain,
    #"\npage_domain: ", page_domain, "\npage_suffix: ", page_suffix)
    filtered_links = set()#[]
    excluded_domains = [page_domain, "", "javascript"]
    excluded_suffixes = [""] #if no suffix, likely not a valid url
    try:
        for link in json_links:
            try:
                #print("link path",link['path'])
                if link['path'] == r"a@/href":# r"A@/href":
                    #print("FOUND A@LINK!!!")
                    link_url = link['url']
                    #print("LINK URL",link_url)
                    link_subdomain, link_domain, link_suffix = tldex.extract(link_url)
                    if link_domain not in excluded_domains:
                        if link_suffix not in excluded_suffixes:
                            if link_subdomain == "":
                                formatted_link = ("",link_domain+"."+link_suffix)
                            else:
                                formatted_link = (link_subdomain,link_domain+"."+link_suffix)
                            filtered_links.add(formatted_link)#,link_url)
            except Exception as e:
                #print("Error in filter_links: ", e)
                pass
        #print("DONE FIltering, results============:\n",filtered_links)
        return filtered_links
    except Exception as e:
        #print("error: ",e)
        pass


def main(sc):
    start_time = time.time()

    parser = argparse.ArgumentParser(description='LinkRun python module')

    # Will likely remobe --wat_number since I get this info in other arguments
    parser.add_argument('--testing_wat',
            default=0,
            type=int,
            help='Used for debugging. If set to 1, will use a testing wat file')
    parser.add_argument('--write_to_db',
            default=0,
            type=int,
            help='Should the job write output to database? (0/1)')
    parser.add_argument('--db_table',
            default="temp",
            type=str,
            help='Specify name of database table to write to. (default=temp)')
    parser.add_argument('--verbose_output_rows',
            default=10,
            type=int,
            help='How many rows of RDD to print to screen for debugging? (default=10)')

    parser.add_argument('--wat_paths_file_s3bucket',
            default='linkrun',
            type=str,
            help='Public S3 bucket with wat.paths file')

    parser.add_argument('--wat_paths_file_s3key',
            default='wat.paths',
            type=str,
            help='Public S3 key pointing to wat.paths file')

    parser.add_argument('--first_wat_file_number',
            default=1,
            type=int,
            help='First row in wat.paths to process (1 is first file)')
    parser.add_argument('--last_wat_file_number',
            default=1,
            type=int,
            help='Last row in wat.paths to process (inclusive, will process this row)')


    parsed_args = parser.parse_args()
    testing_wat = parsed_args.testing_wat
    write_to_db = parsed_args.write_to_db
    db_table = "linkrun."+parsed_args.db_table
    verbose_output_rows = parsed_args.verbose_output_rows
    wat_paths_file_s3bucket = parsed_args.wat_paths_file_s3bucket
    wat_paths_file_s3key = parsed_args.wat_paths_file_s3key
    first_wat_file_number = parsed_args.first_wat_file_number
    last_wat_file_number = parsed_args.last_wat_file_number

    file_location = []
    try:
        s3 = boto3.client('s3')
        s3_object = s3.get_object(Bucket=wat_paths_file_s3bucket, Key=wat_paths_file_s3key)
        current_file = s3_object["Body"]
        current_file_bytestream = BytesIO(current_file.read())
        #current_file_iterator = current_file.iter_lines()
        # skip to the corret line
        with gzip.open(current_file_bytestream, 'rb') as gzip_file:
            if first_wat_file_number > 1:
                gzip_file.readlines(first_wat_file_number-1)

            for item in range(first_wat_file_number,last_wat_file_number+1):
                line = gzip_file.readline().decode('utf-8')
                #line = next(current_file_iterator).decode('utf-8')
                file_location.append("s3a://commoncrawl/"+line.strip())
        file_location = ",".join(file_location)
    except Exception as e:
        print("Couldn't find wat.paths file.\n",e)
    #file_location = "/home/sergey/projects/insight/mainproject_mvp_week2/1/testwat/testwats/testcase3.wat"
    #file_location = "/home/sergey/projects/insight/mainproject/1/testwat/CC-MAIN-20190715175205-20190715200159-00000.warc.wat"

    if testing_wat == 1:
        file_location = "s3a://linkrun/testcase2.wat"
    #file_location = "s3a://commoncrawl/crawl-data/CC-MAIN-2019-30/segments/1563195523840.34/wat/CC-MAIN-20190715175205-20190715200159-00000.warc.wat.gz"

    print("\n","="*10,"FILE LOCATION","="*10)
    for i,name in enumerate(file_location.split(",")):
        print(i,name)


    wat_lines = sc.textFile(file_location)
    #data = wat_lines.take(27)
    #print("27: ",data)
    print("======== Parsing JSON ===="*2)
    rdd = wat_lines.map(lambda x: get_json(x)).filter(lambda x: x != None)\
    .map(lambda json_data: get_json_uri(json_data)).filter(lambda x: x != None)\
    .map(lambda x: ( parse_domain(x[0]),x[0], x[1] )     )\
    .map(lambda x: ( *x[0:-1], get_json_links(x[-1]) )     ).filter(lambda x: x[-1] != None)\
    .map(lambda x: ( *x[0:-1], filter_links(x[-1],*x[0]) )       )\
    .filter(lambda x: ( x[-1] != set() )    )\
    .map(lambda x: (x[0],str(x[0][0]+"."+x[0][1]+"."+x[0][2]),*x[1:]))\
    .flatMap(lambda x: [(z,x[0],*x[1:-1]) for z in x[-1]])\
    .map(lambda x: (x[0],1))\
    .reduceByKey(lambda x,y: x+y)\
    .map(lambda x: (x[0][0],x[0][1],x[1]))


    #.map(lambda x: (x[1],x[0]))#\
    #.sortByKey(0).map(lambda x: (x[1],x[0])) #can do sorting if needed


    ##.map(lambda x: ( *parse_domain(x[0]), x[0], x[1] )     )\ #parse uri domain, uri, json
    #.map(lambda x: print("x0!!:",x[0],"\nX1!!:",x[1]))#(parse_domain(x[0]),x[1]))

    #.map(lambda z: print(type(z)))
    #print("COUNT = ",rdd.count())

    try:
        if write_to_db:
            # Only need if I need datatypes in SparkSQl
            #from pyspark.sql.context import SQLContext
            #from pyspark.sql.types import StructType
            #from pyspark.sql.types import StructField
            #from pyspark.sql.types import StringType
            #from pyspark.sql.types import DecimalType

            #schema = StructType(StringType(),DecimalType())
            #df = SQLContext.createDataFrame(rdd, schema)
            spark = SparkSession(sc)
            df_columns = ["domain","count","subdomain"]
            rdd_df = rdd.toDF()

            if verbose_output_rows != 0:
                rdd_df.show(n=verbose_output_rows)

            mode = "overwrite"
            url = "jdbc:postgresql://linkrundb.caf9edw1merh.us-west-2.rds.amazonaws.com:5432/linkrundb"
            properties = {"user": "postgres","password": "turtles21","driver": "org.postgresql.Driver"}
            rdd_df.write.jdbc(url=url, table=db_table, mode=mode, properties=properties)
    except Exception as e:
        print("DB ERROR ==="*10,"\n>\n",e)
        pass

    # If verbose output is requested:
    if verbose_output_rows != 0:
        #view = rdd.collect()
        view = rdd.take(verbose_output_rows)
        i = 0
        print("\n","="*10,"RDD HEAD","="*10)
        for line in view:
            print(i, line)
            i += 1
            if i == verbose_output_rows: break
        print("="*10,"END OF RDD HEAD","="*10)

    #print(rdd.describe()) ##here working.
    run_time = time.time() - start_time
    print("Total script run time: {}".format(run_time))



if __name__ == "__main__":

    conf = SparkConf()
    sc = SparkContext(conf=conf, appName="LinkRun main module")

    # Set the Credential Keys for AWS S3 Connection
    # Only need if using a non-public s3
    #awsAccessKeyId = "test" #os.environ.get('AWS_ACCESS_KEY_ID')
    #awsSecretAccessKey = "test" #os.environ.get('AWS_SECRET_ACCESS_KEY')
    #sc._jsc.hadoopConfiguration().set('fs.s3n.awsAccessKeyId',awsAccessKeyId)
    #sc._jsc.hadoopConfiguration().set('fs.s3n.awsSecretAccessKey',awsSecretAccessKey)
    #sc._jsc.hadoopConfiguration().set('fs.s3.endpoint','s3.us-east-1.amazonaws.com')
    #sc._jsc.hadoopConfiguration().set('fs.s3.impl','org.apache.hadoop.fs.s3native.NativeS3FileSystem')

    main(sc)
