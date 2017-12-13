-- This script is used to replace the full qualified (and compressed) page idenfieres (urls) with the IDs (longs) created by the Indexer.java class
-- Arcs to URLs which are not represented in the index will be removed
--  
-- Input: 
--	flatten compressed network files
-- Output:
--	flatten indexed network
--	distinct flatten indexed network 

-- Job will be merged into 4 MapReduce Job by Pig Optimization
-- 	1 Load Index
--	2 HashJoin (Index + Network) left side
--	3 HashJoin (Index + Network) right side
--	4 Distinct
--
-- Created by Robert Meusel (robert@informatik.uni-mannheim.de) - 2013-06-03 (ISO 8601)
-- Tested on Pig version 0.10.0 and AWS EMR Pig Version
--
-- Run as follows:
-- pig -p index=/path/to/index/files/ -p input=/path/to/flatten/Network/ -p results=/path/to/results/on/hdfs/ /path/to/this/pig/script/cc-link-extract-pldNetwork.pig


--SET job.priority HIGH;
SET default_parallel 480;

-- use gzip compression in result and intermediary files
SET mapred.output.compress true;
SET mapred.output.compression.codec org.apache.hadoop.io.compress.GzipCodec;
SET pig.tmpfilecompression true;
SET pig.tmpfilecompression.codec gz;
SET mapred.task.timeout 900000;

%default code './ccrdf.jar'; -- our custom UDF lib
--register '$code';

--DEFINE CLEANURLS org.fuberlin.wbsg.ccrdf.pigsupport.CleanUrlsFromString();

%default data '/user/hadoop/wgc/seg1/';
%default index '/user/hadoop/wgc/index/';
%default divider '	'; -- \t

-- load the data
--network = LOAD '$data' USING PigStorage('\n') as (urlstring:chararray);
--index = LOAD '$index' USING PigStorage($divider) as (key:chararray,value:int);

-- clean the data
--clean_network = FOREACH network GENERATE CLEANURLS(urlstring:chararray) as urls;
--cleaned = FOREACH clean_network GENERATE urls.$0 as origin, urls.$1 as links:bag{};
-- filter NULL urls and empty bags
--clfilter = FILTER cleaned BY ((NOT (origin == 'NULL')) AND (NOT (IsEmpty(links)));
--STORE clfilter INTO '$results/cleannetwork';

-- clfilter = LOAD '$data' USING PigStorage($divider) as (origin:chararray, links:bag{T: tuple(C: chararray)});

-- flatten data
--flattennetwork = FOREACH clfilter GENERATE origin, FLATTEN(links) as link;
--STORE flattennetwork INTO '$results/flattennetwork';

-- flattennetwork = LOAD '$data' USING PigStorage($divider) as (origin:chararray, link:chararray);

network = LOAD '$input' USING PigStorage() as (origin:chararray,target:chararray);
index = LOAD '$index' USING PigStorage() as (key:chararray,value:long);

-- join the data
n1 = JOIN network BY origin, index BY key;
n1_filter = FOREACH n1 GENERATE value as origin, target;
n2 = JOIN n1_filter BY target, index by key;
n2_filter = FOREACH n2 GENERATE origin, value as target;
STORE n2_filter INTO '$results/indexedNetwork';

n4 = LOAD '$data' USING PigStorage($divider) as (origin:int, target:int);

networkd = DISTINCT n4;
networks = ORDER networkd BY origin, target;

STORE networks INTO '$results/indexeddistinctsortedNetwork';