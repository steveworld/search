# Clouder Search - MapReduce

Flexible, scalable, fault tolerant, batch oriented system for processing large numbers of records contained in files 
that are stored on HDFS into search indexes stored on HDFS.

# Installation

This step builds the software from source.

<pre>
git clone git@github.com:cloudera/search.git
cd search
#git checkout master
mvn clean package
cd search-mr
ls target/*.jar
</pre>

In addition, below we assume a working MapReduce cluster, for example as installed by Cloudera Manager.

# Configuration

* optionally edit mimetype -> Java parser class mapping in [src/test/resources/tika-config.xml]
* optionally edit file -> mimetype mapping in [src/test/resources/org/apache/tika/mime/custom-mimetypes.xml]
** this file extends and overrides the mappings in the tika-mimetypes.xml file contained in tika-core.jar - [online version](http://github.com/apache/tika/blob/trunk/tika-core/src/main/resources/org/apache/tika/mime/tika-mimetypes.xml)
* optionally edit log levels in src/test/resources/log4j.properties

# MapReduceIndexerTool

MapReduce batch job driver that creates a set of Solr index shards from a set of input files and writes the indexes  into  HDFS, in a flexible, scalable and fault-tolerant manner. 
It also supports merging the output shards into a set of live customer facing Solr servers, typically a SolrCloud.
More details are available through the extensive command line help:

<pre>
$ hadoop jar target/search-mr-*-job.jar org.apache.solr.hadoop.MapReduceIndexerTool --help
</pre>

# HdfsFindTool

HdfsFindTool is essentially the HDFS version of the Linux file system 'find' command. 
The command walks one or more HDFS directory trees and finds all HDFS files that match the specified expression and applies selected actions to them. 
By default, it simply prints the list of matching HDFS file paths to stdout, one path per line. 
The output file list can be piped into the MapReduceIndexerTool via the MapReduceIndexerTool --inputlist option. 
More details are available through the command line help:

<pre>
$ hadoop jar target/search-mr-*-job.jar org.apache.solr.hadoop.HdfsFindTool -help
</pre>
