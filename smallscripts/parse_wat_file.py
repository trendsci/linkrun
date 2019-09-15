#! /usr/bin/python3

import os
import ujson as json
import gzip
import pandas as pd
import tldextract as domex #domain extract

pd.options.display.max_rows = 900
pd.options.display.max_columns = 900
pd.options.display.width = 1000

os.chdir(r"/home/sergey/projects/insight/mainproject/linkrun/cc_data")
current_file = "CC-MAIN-20190715175205-20190715200159-00000.warc.wat.gz"
#print(current_file)

with gzip.open(current_file, 'rb') as cc_wat:
    i = 0
    j = 0
    response_counter = 1
    for k in range(37):
        cc_wat.readline()

    for line in cc_wat:
        if line[0] == 123: #if starts with { it's json
            j += 1
            if response_counter == 2:
                json_data = json.loads(line)
                current_uri = json_data["Envelope"]["WARC-Header-Metadata"]["WARC-Target-URI"]
                print("current URI:", current_uri)

                current_subdomain, current_domain, \
                current_suffix = domex.extract(current_uri)

                print("current domain    = ",current_domain)
                print("current subdomain = ",current_subdomain)
                print("current suffix    = ",current_suffix)
                data_links = json_data["Envelope"]["Payload-Metadata"]["HTTP-Response-Metadata"]["HTML-Metadata"]["Links"]
                df = pd.DataFrame(data_links)
                #print("A@/href only =======")
                ##print(df)
                df_filtered = df[df['path']==r"A@/href"]
                df_filtered = df_filtered[
                df_filtered.apply(lambda x: domex.extract(x['url']).domain != current_domain, axis=1)
                & df_filtered.apply(lambda x: domex.extract(x['url']).domain != "", axis=1) ]

                print(df_filtered.to_string(index=False))



                ##print(json.dumps(json_data))#, indent=4,sort_keys=True))
                print("\n"*2)
                response_counter = -1
            response_counter += 1
        i += 1
        if i == 50: break

print("j= ",j)
