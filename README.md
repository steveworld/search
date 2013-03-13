# Cloudera Search

The sub-directories contain:

* search-flume: Flume sink that extracts search documents from Apache Flume events (using Apache Tika and Solr Cell), transforms them and loads them into Apache Solr. 
* search-mr: Flexible, scalable, fault tolerant, batch oriented system for processing large numbers of records contained in files that are stored on HDFS into search indexes stored on HDFS.
* search-core: common code for search-flume and search-mr, primarily for the Tika Indexer
* search-warc-parser: Apache Tika based WARC (Internet Archive) parser
* search-contrib: additional sources to help with search
* samples: example configurations and test data files