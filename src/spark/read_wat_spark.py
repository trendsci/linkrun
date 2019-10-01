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
import os

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
    to postgres varchar field.

    If input string is not in JSON format, skipps line and returns None.

    Args:
        line: a string in JSON format.

    Returns:
        Dictionary (containing JSON data).
    """
    try:
        line = line.lower()
        # sanitizing input, removing 0x00 character in different encodings (utf, html, etc)
        json_data = json.loads(line.replace(u'\0000', '').replace("&#x0;", "").replace("\x00", "").replace(u'\u0000', ''))
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
                if link['path'] == r"a@/href":
                    link_url = link['url']
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


def main(spark_context):
    """Main function to execute Common Crawl linkback Spark analysis.

    Args:
        sc: spark context.

    Returns:
        None.
    """
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
    parser.add_argument('--jdbc_url',
                        type=str,
                        help='URL to Postgres DB where data will be stored, specifying database')

    parsed_args = parser.parse_args()
    testing_wat = parsed_args.testing_wat
    write_to_db = parsed_args.write_to_db
    db_table = parsed_args.db_table
    verbose_output_rows = parsed_args.verbose_output_rows
    wat_paths_file_s3bucket = parsed_args.wat_paths_file_s3bucket
    wat_paths_file_s3key = parsed_args.wat_paths_file_s3key
    first_wat_file_number = parsed_args.first_wat_file_number
    last_wat_file_number = parsed_args.last_wat_file_number
    jdbc_url = parsed_args.jdbc_url

    # Get passwords from environment variables
    jdbc_password = os.environ['POSTGRES_PASSWORD']
    jdbc_user = os.environ['POSTGRES_USER']

    # Get a list of common crawl file locations to process.
    # This list can be found in common crawl S3 wat.paths file.
    file_location = []
    try:
        s3 = boto3.client('s3')
        s3_object = s3.get_object(Bucket=wat_paths_file_s3bucket, Key=wat_paths_file_s3key)
        current_file = s3_object["Body"]
        current_file_bytestream = BytesIO(current_file.read())

        with gzip.open(current_file_bytestream, 'rb') as gzip_file:
            if first_wat_file_number > 1:
                gzip_file.readlines(first_wat_file_number-1)

            for item in range(first_wat_file_number,last_wat_file_number+1):
                line = gzip_file.readline().decode('utf-8')
                file_location.append("s3a://commoncrawl/"+line.strip())
        # Make a single string with all file locations comma separated.
        file_location = ",".join(file_location)
    except Exception as e:
        print("Couldn't find wat.paths file.\n", e)

    if testing_wat == 1:
        file_location = "s3a://linkrun/testcase2.wat"

    print("\n", "="*10, "FILE LOCATION", "="*10)
    for i, name in enumerate(file_location.split(",")):
        print(i, name)

    # Read all WAT files into Spark RDDs
    wat_lines = spark_context.textFile(file_location)

    # map reduce Spark job:
    print("======== Parsing JSON ========")
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

    if True: #put the try back. this if is for testing
    #try:
        if write_to_db:
            spark = SparkSession(spark_context)
            df_columns = ["domain","count","subdomain"]
            rdd_df = rdd.toDF()

            if verbose_output_rows != 0:
                rdd_df.show(n=verbose_output_rows)

            mode = "overwrite"
            url = jdbc_url
            properties = {"user": jdbc_user, "password": jdbc_password,
                        "driver": "org.postgresql.Driver"}
            rdd_df.write.jdbc(url=url, table=db_table, mode=mode, properties=properties)
    # remove comments, need this except.
    # except Exception as e:
    #     print("DB ERROR ==="*10,"\n>\n",e)
    #     pass

    # If verbose output is requested:
    if verbose_output_rows != 0:
        view = rdd.take(verbose_output_rows)
        print("\n", "="*10, "RDD HEAD", "="*10)
        for line in view:
            print(i, line)
        print("="*10,"END OF RDD HEAD","="*10)

    #print(rdd.describe()) ##here working.

    # Calculate total script run time:
    run_time = time.time() - start_time
    print("Total script run time: {}".format(run_time))


if __name__ == "__main__":
    spark_conf = SparkConf()
    spark_context = SparkContext(conf=spark_conf, appName="LinkRun main module")
    main(spark_context)
