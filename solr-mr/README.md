# Introduction

Scalable, fault tolerant, batch oriented system for processing large numbers of records contained in files 
that are stored on HDFS into free text search indexes stored on HDFS.

For documentation see the [Cloudera Wiki](https://wiki.cloudera.com/display/engineering/Solr+MR).

The main public tools are TikaIndexerTool and HdfsFindShell:

```
usage: hadoop [GenericOptions]... jar solr-mr-*-job.jar 
       [--help] [--inputlist URI] --outputdir HDFS_URI
       --solrhomedir DIR [--mappers INTEGER] [--shards INTEGER]
       [--fairschedulerpool STRING] [--verbose] [--norandomize]
       [--identitytest] [HDFS_URI [HDFS_URI ...]]

Map Reduce job that creates a set of Solr index shards from a list of 
input files and writes the indexes into HDFS, in a scalable and fault-
tolerant manner.

positional arguments:
  HDFS_URI               HDFS URI of file or dir to index (default: [])

optional arguments:
  --help, -h             show this help message and exit
  --inputlist URI        Local URI or HDFS URI of a file containing a list 
                         of HDFS URIs to index, one URI per line. If '-' 
                         is specified, URIs are read from the standard 
                         input. Multiple --inputlist arguments can be 
                         specified
  --outputdir HDFS_URI   HDFS directory to write Solr indexes to
  --solrhomedir DIR      Local dir containing Solr conf/ and lib/
  --mappers INTEGER      Maximum number of MR mapper tasks to use 
                         (default: 1)
  --shards INTEGER       Number of output shards to use (default: 1)
  --fairschedulerpool STRING
                         Name of MR fair scheduler pool to submit job to
  --verbose, -v          Turn on verbose output (default: false)
  --norandomize          undocumented and subject to removal without 
                         notice (default: false)
  --identitytest         undocumented and subject to removal without 
                         notice (default: false)

Generic options supported are
-conf <configuration file>     specify an application configuration file
-D <property=value>            use value for given property
-fs <local|namenode:port>      specify a namenode
-jt <local|jobtracker:port>    specify a job tracker
-files <comma separated list of files>    specify comma separated files to be copied to the map reduce cluster
-libjars <comma separated list of jars>    specify comma separated jar files to include in the classpath.
-archives <comma separated list of archives>    specify comma separated archives to be unarchived on the compute machines.

The general command line syntax is
bin/hadoop command [genericOptions] [commandOptions]

Examples: 

sudo -u hdfs hadoop --config /etc/hadoop/conf.cloudera.mapreduce1 jar solr-mr-*-job.jar  --solrhomedir /home/foo/solr --outputdir hdfs:///user/foo/tikaindexer-output hdfs:///user/foo/tikaindexer-input
```