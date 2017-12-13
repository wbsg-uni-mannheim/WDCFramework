-- This scripts reads in uri lists and makes them distinct and ordered. This step is essential to create an index with this files.
-- Inputfiles are created by cc-link-extract-spareNetwork.pig
--
-- INPUT
--	url lists
-- OUTPUT
--	distinct ordered url list
--
-- Created by Robert Meusel (robert@informatik.uni-mannheim.de) - 2013-05-29 (ISO 8601)
-- Tested on Pig version 0.10.0 and AWS EMR Pig Version
--
-- Run as follows:
-- pig -p uris=s3n://path/to/url/lists/ -p redir=s3n://path/to/url/lists/ -p results=/path/to/results/on/hdfs/ /path/to/this/pig/script/cc-link-extract-pre-index-creator.pig

%default results './cc-results';

--SET job.priority HIGH;
--SET default_parallel 240;

-- use gzip compression in result and intermediary files
SET mapred.task.timeout 900000;
SET mapred.output.compress true;
SET mapred.output.compression.codec org.apache.hadoop.io.compress.GzipCodec;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec gz;

%default uris '/user/hadoop/wgc/urls/';
-- load the data
crawled_uris = LOAD '$uris' USING PigStorage() as (origin:chararray);

-- make distinct list

uris_distinct = DISTINCT crawled_uris;
list = ORDER uris_distinct BY origin;
STORE list INTO '$results/distinctOrderedCrawledUriList';