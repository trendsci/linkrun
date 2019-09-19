# LinkRun
A pipeline to analyze popularity of links across the web.

## Idea

LinkRun is a data engineering project created by me during the Insight Data Engineering fellowship.<br>
LinkRun scans data from millions of web pages across the web and collects all the links present on those pages. LinkRun then counts how many times each web domain was linked to and presents this score per domain name.<br>

Example output:

| LINK  | COUNT |
|-------|-------|
| www.google.com  | 4,321,555     |
| www.youtube.com | 3,125,484     |
| ... | ...     |
| www.YourSite.com| 20,915        |


## The Pipeline

<img src="./graphics/LinkRunPipeline.png" alt="LinkRun Pipeline" width="800"/><br>

## The Data

LinkRun uses publically available website crawl data from [Common Crawl database](https://commoncrawl.org/). Common Crawl data is updated once a month with new web crawl data. LinkRun uses a scheduler that automatically reads and processes new data as it is made available.<br>

## How to run LinkRun on your own

LinkRun can run on any resource that supports the applications used in the pipline. For best results, LinkRun should be run on AWS provisioned resources.<br>
To automate AWS resource provisioning and setup the [Pegasus automatic deployment platform](https://github.com/InsightDataScience/pegasus) is used.<br>
The following modifications are introduced to the Pegasus scripts to add required dependencies for LinkRun:
* Replace /install/exvironment/install_env.sh with the file provided in this repo.


Instructions to go here...
