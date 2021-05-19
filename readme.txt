#WDCFramework
Java Framework which is used by the Web Data Commons project to extract Microdata, Microformats and RDFa data, Web graphs, and HTML tables from the web crawls provided by the Common Crawl Foundation.

More details:
http://webdatacommons.org/framework/

CommonCrawl Structured Data Extraction - Documentation

This implementation extracts structured web data in various formats from the Common Crawl web corpus by using an AWS pipeline.
The data is given in an EC2 bucket and consists of a large number of web pages, which is split into a number of archive files.
http://commoncrawl.org/data/accessing-the-data/

The setup is to use a SQS queue for extraction tasks, where each queue entry contains a single data file.
A number of extraction EC2 instances monitors this queue, and performs the actual extraction. 
Results are again written into EC2 (data) and SDB (statistics)

Use as follows (For a detailed description visit http://webdatacommons.org/framework):

0) Create a ccrdf.properties file in /src/main/resources. A sample file is provided (ccrdf.properties.dist).

1) Create a runnable JAR CC extractor JAR file
This is done by running
mvn install
in this directory. This creates a JAR file ccrdf-*.jar in the "target/" subdirectory.

2) Use 'deploy' command to upload the JAR to S3
./bin/master deploy --jarfile target/ccrdf-*.jar

3) Use 'queue' command to fill the extraction queue with CC file names
./bin/master queue --bucket-prefix CC-MAIN-2013-48/segments/1386163041297/wet/

The prefix can be used to select a subset of the data, in this case only data from 2010-08-09.

4) Use 'start' command to launch EC2 extraction instances from the spot market. This request will keep starting instances until it is cancelled, so beware! Also, the price limit has to be given. The current spot prices can be found at http://aws.amazon.com/ec2/spot-instances/#6 . A general recommendation is to set this price at about the on-demand instance price. This way, we will benefit from the generally low spot prices without our extraction process being permanently killed. The price limit is given in US$.
./bin/master start --worker-amount 10 --pricelimit 0.6
Note: it may take a while (observed: ca. 10-15 Minutes) for the instances to become available and start taking tasks from the queue. 

5) Wait until everything is finished using the 'monitor' command
./bin/master monitor
The monitoring process will try to guess the extraction speed and give an estimate on the remaining time. Also, the number of currently running instances is displayed.
 
6) Use 'shutdown' command to kill worker nodes and terminate the spot instance request
./bin/master shutdown

7) Collect result statistics with the 'retrievestats' command
./bin/master retrievestats --destination /some/directory

8) (Optional) Collect result data with the 'retrievesdata' command
./bin/master retrievedata --destination /some/directory --> this is only necessary or possible if you have implemented a new DataThread to gather your data - otherwise simply download the data.

Both the data and the statistics will be stored in this directory

To reset the process, use the 'clearqueue' command to reset the data queue and 'cleardata' to remove intermediate extraction results and statistics
./bin/master clearqueue
./bin/master cleardata

Note that you have to wait 60 seconds before you can reissue the 'queue' command after 'clearqueue'



