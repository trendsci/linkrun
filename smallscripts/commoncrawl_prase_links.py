#! /usr/bin/python3

import traceback
import ujson as json
import tldextract as domex #domain extract
import pandas as pd
#pd.set_option('display.max_rows',900)
pd.options.display.max_rows = 900
pd.options.display.max_columns = 900
pd.options.display.width = 1000

print("Started python script")
path = r"/home/sergey/projects/insight/mainproject/commoncrawl/84c2ba88f1a3f27bb1353e5ff92032b5-313b675a01ee1d1f05829439165a9eb991571547/"
path += r"bbc.wat"
header = 9

def main():
    #try:
    with open(path, 'r') as f_json:
        # skip header lines
        # can actually read line #3 which contains URI of page
        for i in range(header): f_json.readline()
        result = json.load(f_json)

    data_current_uri = result["Envelope"]["WARC-Header-Metadata"]["WARC-Target-URI"]
    print("current uri    =",data_current_uri)
    print("current domain =",domex.extract(data_current_uri).domain)
    data_links = result["Envelope"]["Payload-Metadata"]["HTTP-Response-Metadata"]["HTML-Metadata"]["Links"]
    #print(data_links)
    df = pd.DataFrame(data_links)
    print("A@/href")
    df_filtered = df[df['path']==r"A@/href"]
    df_filtered = df_filtered[
    df_filtered.apply(lambda x: domex.extract(x['url']).domain != "bbc" , axis=1)
    & df_filtered.apply(lambda x: domex.extract(x['url']).domain != "" , axis=1) ]

    print(df_filtered.head(55))
    print(df_filtered.shape)

if __name__ == "__main__":
    main()
