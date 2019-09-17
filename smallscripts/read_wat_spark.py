#! /usr/bin/python3

from pyspark import SparkConf, SparkContext
import ujson as json
import tldextract as tldex

def get_json(line):
    try:
        json_data = json.loads(line)
        return json_data
    except:
        pass


def get_json_uri(json_line):
    try:
        current_uri = json_line["Envelope"]["WARC-Header-Metadata"]["WARC-Target-URI"]
        #print("current uri: ",current_uri)
        return (current_uri, json_line)
    except Exception as e:
        pass
        #print("No Target URI")
        #print("error: ",e)

def parse_domain(uri):
    try:
        print("URI!! ",type(uri),"\n",uri)
        subdomain, domain, suffix = tldex.extract(uri)
        print("parsed!!: ",subdomain, domain, suffix)
        return subdomain, domain, suffix
    except Exception as e:
        print("error:",e)
        pass

def get_json_links(json_line):
    try:
        links = json_line["Envelope"]["Payload-Metadata"]["HTTP-Response-Metadata"]["HTML-Metadata"]["Links"]
        return links
    except Exception as e:
        print("error: ",e)
        pass

def filter_links(json_links, page_subdomain, page_domain, page_suffix):
    filtered_links = []
    try:
        for link in json_links:
            link_subdomain, link_domain, link_suffix = tldex.extract(link)
            if link_domain != page_domain:
                filtered_links.append(link_domain)
        return filtered_links
    except:
        pass


def main(sc):
#    s3file = "s3://commoncrawl/crawl-data/CC-MAIN-2019-30/segments/1563195523840.34/wat/CC-MAIN-20190715175205-20190715200159-00024.warc.wat.gz"
    file_location = "/home/sergey/projects/insight/mainproject/1/testwat/head.wat"
    wat_lines = sc.textFile(file_location)
    #data = wat_lines.take(27)
    #print("27: ",data)
    print("======== Parsing JSON ===="*2)
    json_data = wat_lines.map(lambda x: get_json(x)).filter(lambda x: x != None)\
    .map(lambda json_data: get_json_uri(json_data)).filter(lambda x: x != None)\
    .map(lambda x: ( *parse_domain(x[0]), x[0], x[1] )     )\
    .map(lambda x: ( *x[0:-1], get_json_links(x[-1]) )     )\
    .map(lambda x: ( *x[0:-1], filter_links(x[-1],*x[:3]) )       )
    #.map(lambda x: print("x0!!:",x[0],"\nX1!!:",x[1]))#(parse_domain(x[0]),x[1]))

    #.map(lambda z: print(type(z)))
    print(json_data.take(3))


if __name__ == "__main__":
    conf = SparkConf()
    sc = SparkContext(conf=conf)

    # Set the Credential Keys for AWS S3 Connection
    awsAccessKeyId = "test" #os.environ.get('AWS_ACCESS_KEY_ID')
    awsSecretAccessKey = "test" #os.environ.get('AWS_SECRET_ACCESS_KEY')
    sc._jsc.hadoopConfiguration().set('fs.s3n.awsAccessKeyId',awsAccessKeyId)
    sc._jsc.hadoopConfiguration().set('fs.s3n.awsSecretAccessKey',awsSecretAccessKey)
    sc._jsc.hadoopConfiguration().set('fs.s3.endpoint','s3.us-east-1.amazonaws.com')
    sc._jsc.hadoopConfiguration().set('fs.s3.impl','org.apache.hadoop.fs.s3native.NativeS3FileSystem')

    main(sc)
