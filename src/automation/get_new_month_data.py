#! /usr/bin/python3
"""
This module reads in the common crawl index and
saves to file the most recent crawl available.
You can check the content of the output file to
find out what the latest is the latest available crawl data.

"""


# import boto3

import ujson as json
import requests


def main():
    """Fethces latest crawl info from common crawl.

    This function reads in the common crawl index and
    saves to file the most recent crawl available.

    Args:
        None

    Returns:
        JSON dict with the informatio about the latest
        crawl by common crawl. For example:

        {'id': 'CC-MAIN-2019-35', 'name': 'August 2019 Index',
        'timegate': 'http://index.commoncrawl.org/CC-MAIN-2019-35/',
        'cdx-api': 'http://index.commoncrawl.org/CC-MAIN-2019-35-index'}

    """
    commoncrawl_json_index_webpage = requests.get(
        'http://index.commoncrawl.org/collinfo.json')

    commoncrawl_index_json = commoncrawl_json_index_webpage.json()

    latest_commoncrawl = commoncrawl_index_json[0]

    with open('commoncrawl_newest_crawl', 'w') as f:
        json.dump(latest_commoncrawl,f, escape_forward_slashes = False)

    #with open('commoncrawl_newest_crawl','r') as f:
    #    a = json.load(f)
    #    print("File loaded\n",a)

    return latest_commoncrawl

if __name__ == "__main__":
    main()
