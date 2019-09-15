#! /usr/bin/python3

import os
import ujson as json
import gzip
import pandas as pd
import tldextract as domex #domain extract
import time

pd.options.display.max_rows = 900
pd.options.display.max_columns = 900
pd.options.display.width = 1000
pd.options.display.max_colwidth = -1

start_time = time.time()

os.chdir(r"/home/sergey/projects/insight/mainproject/linkrun/cc_data")
current_file = "CC-MAIN-20190715175205-20190715200159-00000.warc.wat.gz"
#print(current_file)

with gzip.open(current_file, 'rb') as cc_wat:
    line_number = 0
    j = 0
    pages_with_any_links = 0
    #pages_with_links_other_than_to_self = 0
    response_counter = 1
    for k in range(37):
        cc_wat.readline()
        line_number += 1

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
                try:
                    data_links = json_data["Envelope"]["Payload-Metadata"]["HTTP-Response-Metadata"]["HTML-Metadata"]["Links"]
                    pages_with_any_links += 1
                except Exception as e:
                    pass
                    # if you need more info about entries
                    # without URLs
                    # these are likely
                    # "WARC-Identified-Payloa   1138 d-Type":"image/png"
                    # e.g. WARC-Refers-To: <urn:uuid:94ddbb0b-ed3a-4487-8296-532c8f80fb0d>
                    if False:
                        print(json_data)
                        print(data_links)
                        print("="*20,"error: ",e)
                        break
                df = pd.DataFrame(data_links)
                #print("A@/href only =======")
                ##print(df)
                df_filtered = df[df['path']==r"A@/href"]
                #print(df_filtered['url'][:10])
                df_filtered = df_filtered[
                df_filtered.apply(lambda x: domex.extract(x['url']).domain != current_domain, axis=1) #remove links that link-back to current domain
                & df_filtered.apply(lambda x: domex.extract(x['url']).domain != "", axis=1) #remove links with empty domain
                & df_filtered.apply(lambda x: x['url'][:10] != "javascript", axis = 1) #remove links to javascript functions
                & df_filtered.apply(lambda x: domex.extract(x['url']).suffix != "", axis=1)] #remove links to pages like "index.html" which are on same domain

                print(df_filtered[['url']].to_string(index=False))



                ##print(json.dumps(json_data))#, indent=4,sort_keys=True))
                print("\n"*2)
                response_counter = -1
            response_counter += 1
        line_number += 1
        if line_number == 500: break

print("j= ",j)
run_time = time.time() - start_time
print("Total script run time: {}".format(run_time))
