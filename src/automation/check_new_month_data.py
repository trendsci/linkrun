#! /usr/bin/python3
"""
This module doc string here
"""


# import boto3

import ujson as json
import requests


def main():
    """
    Doc test here
    """
    commoncrawl_json_index_webpage = requests.get(
        'http://index.commoncrawl.org/collinfo.json')

    commoncrawl_index_json = commoncrawl_json_index_webpage.json()

    latest_commoncrawl = commoncrawl_index_json[0]

    with open('commoncrawl_newest_crawl', 'w') as f:
        json.dump(latest_commoncrawl,f, escape_forward_slashes = False)

    with open('commoncrawl_newest_crawl','r') as f:
        a = json.load(f)
        print("File loaded\n",a)

if __name__ == "__main__":
    main()
