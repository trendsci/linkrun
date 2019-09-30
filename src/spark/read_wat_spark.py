#! /usr/bin/python3
"""
This script reads Common Crawl WAT files, parses and analyzez the JSON data
from WAT files and outputs, and produces a list of number of
linkbacks per domain (at a subdomain granularity).

This job uses Spark to run this distributed ETL job.
"""

import time
import gzip
import argparse
from io import BytesIO

import ujson as json
import tldextract as tldex
import boto3

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


def get_json(line):
    """Reads in string and returns JSON dictionary object.

    Reads a string, converts it to lowercase,
    replaces UTF8 0x00 (null) characters with '' (empty string),
    and returns a JSON dictionay object.
    Replacing 0x00 characters is required since they cannot be written
    to postgres.

    If input string is not in JSON format, skipps line and returns None.

    Args:
        line: a string in JSON format.

    Returns:
        Dictionary (containing JSON data).
    """
    try:
        line = line.lower()
        json_data = json.loads(line.replace(u'\0000', ''))
        return json_data
    except Exception as e:
        pass


def get_json_uri(json_line):
    """Returns the URI of page from JSON data.

    Args:
        json_line: dictionary with JSON data.

    Returns:
        (current_uri, json_line): the current URI,
            and the original JSON dictionary.
    """
    try:
        current_uri = json_line["envelope"]["warc-header-metadata"]["warc-target-uri"]
        return (current_uri, json_line)
    except Exception as e:
        pass

def parse_domain(uri):
    """Parse URI into subdomain, domain, and suffix (top level domain).

    Args:
        URI: full URI

    Returns:
        subdomain, domain, suffix (top level domain)
    """
    try:
        subdomain, domain, suffix = tldex.extract(uri)
        return subdomain, domain, suffix
    except Exception as e:
        #print("error:",e)
        pass

def get_json_links(json_line):
    """Return a list of links from the WAT JSON data.

    Args:
        json_line: dictionary with JSON data.

    Returns:
        list of dictionaries each containing a link URL, tag type, and text.
    """
    try:
        links = json_line["envelope"]["payload-metadata"]["http-response-metadata"]["html-metadata"]["links"]
        return links
    except Exception as e:
        #print("error: ",e)
        pass

def filter_links(json_links, page_subdomain, page_domain, page_suffix):
    """Filters out links based on set criteria.

    Filters out all links that are not in an <a href='...'>. Filters out links
    that point to the same domain as the original page they are on, links that
    have no domain (i.e. they point to pages on the same domain as the current
    page), links that call javascript functions, and links without a suffix (no
    top level domain).

    Args:
        json_links: links in JSON format.
        page_subdomain: current page subdomain.
        page_domain: current page domain.
        page_suffix: curernt page suffix (top level domain)

    Returns:
        Set containing tuples of (link_subdomain, link_domain_and_suffix).
    """
    filtered_links = set()
    excluded_domains = [page_domain, "", "javascript"]
    excluded_suffixes = [""]  # if there's no suffix, likely not a valid url
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
                                formatted_link = ("", link_domain+"."+link_suffix)
                            else:
                                formatted_link = (link_subdomain, link_domain+"."+link_suffix)
                            filtered_links.add(formatted_link)#,link_url)
            except Exception as e:
                #print("Error in filter_links: ", e)
                pass
        return filtered_links
    except Exception as e:
        #print("error: ",e)
        pass


def main(sc):


    start_time = time.time()

    parser = argparse.ArgumentParser(description='LinkRun python module')
    parser.add_argument('--testing_wat',
                        default=0,
                        type=int,
                        help='Used for debugging. If set to 1, will use a testing wat file (default=0)')
    parser.add_argument('--write_to_db',
                        default=0,
                        type=int,
                        help='Should the job write output to database? (0/1), (default=0)')
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
    if True: #put the try back. this if is for testing
    #try:
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
    # remove comments, need this except.
    # except Exception as e:
    #     print("DB ERROR ==="*10,"\n>\n",e)
    #     pass

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
    main(sc)
