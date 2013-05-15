# Cloudera Search - MapReduce

Flexible, scalable, fault tolerant, batch oriented system for processing large numbers of records contained in files 
that are stored on HDFS into search indexes stored on HDFS; Uses a morphline.

## Getting Started

The steps below assume you have already [built the software](../README.md).
In addition, below we assume a working MapReduce cluster, for example as installed by Cloudera Manager.

## MapReduceIndexerTool

MapReduce batch job driver that creates a set of Solr index shards from a set of input files and writes the indexes  into  HDFS, in a flexible, scalable and fault-tolerant manner. 
It also supports merging the output shards into a set of live customer facing Solr servers, typically a SolrCloud.
More details are available through the extensive command line help:

<pre>
$ hadoop jar target/search-mr-*-job.jar org.apache.solr.hadoop.MapReduceIndexerTool --help

usage: hadoop [GenericOptions]... jar search-mr-*-job.jar org.apache.solr.hadoop.MapReduceIndexerTool
       [--help] --output-dir HDFS_URI [--input-list URI]
       --morphline-file FILE [--morphline-id STRING] [--solr-home-dir DIR]
       [--update-conflict-resolver FQCN] [--mappers INTEGER]
       [--reducers INTEGER] [--max-segments INTEGER]
       [--fair-scheduler-pool STRING] [--dry-run] [--verbose]
       [--shard-url URL] [--zk-host STRING] [--shards INTEGER] [--go-live]
       [--collection STRING] [--go-live-threads INTEGER]
       [HDFS_URI [HDFS_URI ...]]

MapReduce batch job driver that creates a  set of Solr index shards from a
set of input files  and  writes  the  indexes  into  HDFS,  in a flexible,
scalable and fault-tolerant manner.  It  also  supports merging the output
shards into a  set  of  live  customer  facing  Solr  servers, typically a
SolrCloud. The program  proceeds  in  several  consecutive MapReduce based
phases, as follows:

1) Randomization phase:  This  (parallel)  phase  randomizes  the  list of
input files in  order  to  spread  indexing  load  more  evenly  among the
mappers of the subsequent phase.

2) Mapper phase: This  (parallel)  phase  takes  the input files, extracts
the relevant content, transforms it and  hands SolrInputDocuments to a set
of reducers. The  ETL  functionality  is  flexible  and customizable using
chains  of  arbitrary  morphline  commands  that  pipe  records  from  one
transformation command to another. Commands  to  parse and transform a set
of standard data formats such as  Avro,  CSV,  Text, HTML, XML, PDF, Word,
Excel, etc. are provided out  of  the  box, and additional custom commands
and parsers for additional file or data  formats can be added as morphline
plugins. This  is  done  by  implementing  a  simple  Java  interface that
consumes a record (e.g. a file  in  the  form  of an InputStream plus some
headers plus contextual metadata)  and  generates  as  output zero or more
records. Any kind of data  format  can  be  indexed and any Solr documents
for any kind of Solr schema  can  be  generated,  and any custom ETL logic
can be registered and executed.
Optionally, rich  input  files  can  be  mapped  to  MIME  types  via  the
detectMimeType morphline command, i.e. by  specifying  to include the Tika
defaultMimeTypes config file (which  already  ships embedded in tika-core.
jar       -       see       http://github.com/apache/tika/blob/trunk/tika-
core/src/main/resources/org/apache/tika/mime/tika-mimetypes.xml)       and
optionally also one  or  more  custom-mimetypes.xml  configs  (either as a
file or embedded  XML  fragment),  which  extends  and  overrides the Tika
defaultMimeTypes with custom directives.
Morphline commands can use MIME  types  to  determine how to interpret the
input data. 
Fields, including MIME types, can also  explicitly be passed by force from
the CLI to the  morphline,  for  example:  hadoop  ... -D org.apache.solr.
hadoop.morphline.MorphlineMapRunner.field._attachment_mimetype=text/csv

3)   Reducer   phase:   This   (parallel)   phase   loads   the   mapper's
SolrInputDocuments into  one  EmbeddedSolrServer  per  reducer.  Each such
reducer and Solr server can be seen  as  a (micro) shard. The Solr servers
store their data in HDFS.

4) Mapper-only merge  phase:  This  (parallel)  phase  merges  the  set of
reducer shards into the number of solr  shards expected by the user, using
a mapper-only job. This  phase  is  omitted  if  the  number  of shards is
already equal to the number of shards expected by the user. 

5) Go-live phase: This optional (parallel)  phase merges the output shards
of the previous phase into  a  set  of  live customer facing Solr servers,
typically a SolrCloud. If this phase  is  omitted you can explicitly point
each Solr server to one of the HDFS output shard directories.

Fault Tolerance: Mapper and reducer  task  attempts are retried on failure
per the standard MapReduce semantics. On  program  startup all data in the
--output-dir is deleted if that  output  directory  already exists. If the
whole job fails you can retry simply  by rerunning the program again using
the same arguments.

positional arguments:
  HDFS_URI               HDFS URI of  file  or  directory  tree  to index.
                         (default: [])

optional arguments:
  --help, -help, -h      Show this help message and exit
  --input-list URI       Local URI or HDFS  URI  of  a  UTF-8 encoded file
                         containing a list of HDFS  URIs to index, one URI
                         per line in the file.  If  '-' is specified, URIs
                         are read from  the  standard  input.  Multiple --
                         input-list arguments can be specified.
  --morphline-id STRING  The identifier of  the  morphline  that  shall be
                         executed  within   the   morphline   config  file
                         specified  by   --morphline-file.   If   the   --
                         morphline-id option is  ommitted  the first (i.e.
                         top-most) morphline  within  the  config  file is
                         used. Example: morphline1
  --solr-home-dir DIR    Relative  or  absolute  path   to   a  local  dir
                         containing  Solr  conf/  dir  and  in  particular
                         conf/solrconfig.xml  and  optionally   also  lib/
                         dir. This directory will  be  uploaded to each MR
                         task. Example: src/test/resources/solr/minimr
  --update-conflict-resolver FQCN
                         Fully qualified class name  of  a Java class that
                         implements the  UpdateConflictResolver interface.
                         This enables  deduplication  and  ordering  of  a
                         series of document  updates  for  the same unique
                         document key. For example,  a MapReduce batch job
                         might index multiple files in  the same job where
                         some of the files  contain  old  and new versions
                         of the very same document,  using the same unique
                         document key.
                         Typically,  implementations  of   this  interface
                         forbid collisions by  throwing  an  exception, or
                         ignore all but the  most recent document version,
                         or, in the general  case, order colliding updates
                         ascending  from  least  recent   to  most  recent
                         (partial) update. The  caller  of  this interface
                         (i.e. the Hadoop  Reducer)  will  then  apply the
                         updates to Solr  in  the  order  returned  by the
                         orderUpdates() method.
                         The                                       default
                         RetainMostRecentUpdateConflictResolver
                         implementation ignores all  but  the  most recent
                         document  version,   based   on   a  configurable
                         numeric  Solr  field,   which   defaults  to  the
                         file_last_modified   timestamp   (default:   org.
                         apache.solr.hadoop.dedup.
                         RetainMostRecentUpdateConflictResolver)
  --mappers INTEGER      Tuning knob that indicates  the maximum number of
                         MR mapper tasks to use.  -1 indicates use all map
                         slots available on the cluster. (default: -1)
  --reducers INTEGER     Tuning  knob  that   indicates   the   number  of
                         reducers to  index  into.  -1  indicates  use all
                         reduce  slots  available   on   the   cluster.  0
                         indicates  use  one  reducer  per  output  shard,
                         which disables the mtree  merge MR algorithm. The
                         mtree merge MR algorithm  improves scalability by
                         spreading load (in particular  CPU  load) among a
                         number of  parallel  reducers  that  can  be much
                         larger than the  number  of  solr shards expected
                         by the user. It can  be  seen  as an extension of
                         concurrent  lucene  merges   and   tiered  lucene
                         merges to  the  clustered  case.  The  subsequent
                         mapper-only  phase  merges  the  output  of  said
                         large number of reducers to  the number of shards
                         expected by the  user,  again  by  utilizing more
                         available parallelism on  the  cluster. (default:
                         -1)
  --max-segments INTEGER
                         Tuning knob that indicates  the maximum number of
                         segments to be contained  on  output in the index
                         of each reducer shard. After  a reducer has built
                         its output index  it  applies  a  merge policy to
                         merge segments  until  there  are  <= maxSegments
                         lucene  segments  left  in  this  index.  Merging
                         segments involves reading and  rewriting all data
                         in all these segment  files, potentially multiple
                         times, which  is  very  I/O  intensive  and  time
                         consuming. However, an index  with fewer segments
                         can later be merged faster,  and  it can later be
                         queried faster  once  deployed  to  a  live  Solr
                         serving shard. Set maxSegments  to  1 to optimize
                         the index for low  query  latency. In a nutshell,
                         a  small   maxSegments   value   trades  indexing
                         latency for subsequently  improved query latency.
                         This can  be  a  reasonable  trade-off  for batch
                         indexing systems. (default: 1)
  --fair-scheduler-pool STRING
                         Optional tuning knob that  indicates  the name of
                         the fair scheduler pool  to  submit  jobs to. The
                         Fair   Scheduler   is   a   pluggable   MapReduce
                         scheduler that  provides  a  way  to  share large
                         clusters.  Fair  scheduling   is   a   method  of
                         assigning resources to  jobs  such  that all jobs
                         get, on  average,  an  equal  share  of resources
                         over time. When there  is  a  single job running,
                         that job  uses  the  entire  cluster.  When other
                         jobs are submitted, tasks slots  that free up are
                         assigned to the new jobs,  so  that each job gets
                         roughly the same amount  of  CPU time. Unlike the
                         default Hadoop scheduler, which  forms a queue of
                         jobs, this lets short  jobs  finish in reasonable
                         time while not starving long  jobs. It is also an
                         easy way to share  a  cluster between multiple of
                         users.  Fair  sharing  can  also  work  with  job
                         priorities - the priorities  are  used as weights
                         to determine the fraction  of  total compute time
                         that each job gets.
  --dry-run              Run in local mode  and  print documents to stdout
                         instead of loading them  into Solr. This executes
                         the morphline  in  the  client  process  (without
                         submitting a job  to  MR)  for quicker turnaround
                         during early trial  &  debug  sessions. (default:
                         false)
  --verbose, -v          Turn on verbose output. (default: false)

Required arguments:
  --output-dir HDFS_URI  HDFS directory to write  Solr  indexes to. Inside
                         there one  output  directory  per  shard  will be
                         generated.    Example:    hdfs://c2202.mycompany.
                         com/user/$USER/test
  --morphline-file FILE  Relative or absolute path to  a local config file
                         that contains one  or  more  morphlines. The file
                         must     be      UTF-8      encoded.     Example:
                         /path/to/morphline.conf

Cluster arguments:
  Arguments that provide information about  your  Solr cluster. If you are
  not using --go-live, pass  the  --shards  argument.  If you are building
  shards for a Non-SolrCloud  cluster,  pass  the --shard-url argument one
  or more times. To build indexes  for  a replicated cluster with --shard-
  url, pass replica urls consecutively and  also pass --shards. If you are
  building shards for a  SolrCloud  cluster,  pass the --zk-host argument.
  Using --go-live requires either --shard-url or --zk-host.

  --shard-url URL        Solr URL to merge  resulting  shard into if using
                         --go-live. Example: http://solr001.mycompany.com:
                         8983/solr/collection1.    Multiple    --shard-url
                         arguments can be specified,  one for each desired
                         shard.  If  you   are   merging   shards  into  a
                         SolrCloud cluster, use --zk-host instead.
  --zk-host STRING       The address of  a  ZooKeeper  ensemble being used
                         by a SolrCloud  cluster.  This ZooKeeper ensemble
                         will be  examined  to  determine  the  number  of
                         output shards to create as  well as the Solr URLs
                         to merge the output shards into when using the --
                         go-live option. Requires that  you  also pass the
                         --collection to merge the shards into.
                         
                         The  --zk-host   option   implements   the   same
                         partitioning semantics as  the standard SolrCloud
                         Near-Real-Time (NRT)  API.  This  enables  to mix
                         batch  updates  from   MapReduce  ingestion  with
                         updates from standard Solr  NRT  ingestion on the
                         same SolrCloud  cluster,  using  identical unique
                         document keys.
                         
                         Format is: a  list  of  comma separated host:port
                         pairs,  each  corresponding   to   a  zk  server.
                         Example:               '127.0.0.1:2181,127.0.0.1:
                         2182,127.0.0.1:2183'  If   the   optional  chroot
                         suffix is  used  the  example  would  look  like:
                         '127.0.0.1:2181/solr,127.0.0.1:2182/solr,
                         127.0.0.1:2183/solr' where  the  client  would be
                         rooted  at  '/solr'  and   all   paths  would  be
                         relative     to     this      root     -     i.e.
                         getting/setting/etc...  '/foo/bar'  would  result
                         in operations being run  on '/solr/foo/bar' (from
                         the server perspective).
                         
                         If --solr-home-dir  is  not  specified,  the Solr
                         home  directory  for   the   collection  will  be
                         downloaded from this ZooKeeper ensemble.
  --shards INTEGER       Number of output shards to generate.

Go live arguments:
  Arguments for merging  the  shards  that  are  built  into  a  live Solr
  cluster. Also see the Cluster arguments.

  --go-live              Allows you to  optionally  merge  the final index
                         shards into a live  Solr  cluster  after they are
                         built. You can pass the ZooKeeper address with --
                         zk-host  and  the  relevant  cluster  information
                         will be auto detected.  If  you  are  not using a
                         SolrCloud cluster, --shard-url  arguments  can be
                         used to  specify  each  SolrCore  to  merge  each
                         shard into. (default: false)
  --collection STRING    The SolrCloud  collection  to  merge  shards into
                         when  using  --go-live  and  --zk-host.  Example:
                         collection1
  --go-live-threads INTEGER
                         Tuning knob that indicates  the maximum number of
                         live merges  to  run  in  parallel  at  one time.
                         (default: 1000)

Generic options supported are
  --conf <configuration file>
                         specify an application configuration file
  -D <property=value>    use value for given property
  --fs <local|namenode:port>
                         specify a namenode
  --jt <local|jobtracker:port>
                         specify a job tracker
  --files <comma separated list of files>
                         specify comma separated  files  to  be  copied to
                         the map reduce cluster
  --libjars <comma separated list of jars>
                         specify comma separated jar  files  to include in
                         the classpath.
  --archives <comma separated list of archives>
                         specify   comma   separated    archives   to   be
                         unarchived on the compute machines.

The general command line syntax is
bin/hadoop command [genericOptions] [commandOptions]

Examples: 

# Prepare a config jar file containing a custom mylog4j.properties:
rm -fr myconfig; mkdir myconfig
cp src/test/resources/log4j.properties myconfig/mylog4j.properties
cp -r src/test/resources/org myconfig/
jar -cMvf myconfig.jar -C myconfig .

# (Re)index an Avro based Twitter tweet file:
sudo -u hdfs hadoop \
  --config /etc/hadoop/conf.cloudera.mapreduce1 \
  jar target/search-mr-*-job.jar org.apache.solr.hadoop.MapReduceIndexerTool \
  --libjars myconfig.jar \
  -D 'mapred.child.java.opts=-Xmx500m -Dlog4j.configuration=mylog4j.properties' \
  --morphline-file ../search-core/src/test/resources/test-morphlines/tutorialReadAvroContainer.conf \
  --solr-home-dir src/test/resources/solr/minimr \
  --output-dir hdfs://c2202.mycompany.com/user/$USER/test \
  --shards 1 \
  hdfs:///user/$USER/test-documents/sample-statuses-20120906-141433.avro

# (Re)index all files that match all of the following conditions:
# 1) File is contained in dir tree hdfs:///user/$USER/solrloadtest/twitter/tweets
# 2) file name matches the glob pattern 'sample-statuses*.gz'
# 3) file was last modified less than 100000 minutes ago
# 4) file size is between 1 MB and 1 GB
# Also include extra library jar file containing JSON tweet Java parser:
hadoop jar target/search-mr-*-job.jar org.apache.solr.hadoop.HdfsFindTool \
  -find hdfs:///user/$USER/solrloadtest/twitter/tweets \
  -type f \
  -name 'sample-statuses*.gz' \
  -mmin -1000000 \
  -size -100000000c \
  -size +1000000c \
| sudo -u hdfs hadoop \
  --config /etc/hadoop/conf.cloudera.mapreduce1 \
  jar target/search-mr-*-job.jar org.apache.solr.hadoop.MapReduceIndexerTool \
  --libjars myconfig.jar \
  -D 'mapred.child.java.opts=-Xmx500m -Dlog4j.configuration=mylog4j.properties' \
  --morphline-file ../search-core/src/test/resources/test-morphlines/tutorialReadJsonTestTweets.conf \
  --solr-home-dir src/test/resources/solr/minimr \
  --output-dir hdfs://c2202.mycompany.com/user/$USER/test \
  --shards 100 \
  --input-list -

# Go live by merging resulting index shards into a live Solr cluster
# (explicitly specify Solr URLs - for a SolrCloud cluster see next example):
sudo -u hdfs hadoop \
  --config /etc/hadoop/conf.cloudera.mapreduce1 \
  jar target/search-mr-*-job.jar org.apache.solr.hadoop.MapReduceIndexerTool \
  --libjars myconfig.jar \
  -D 'mapred.child.java.opts=-Xmx500m -Dlog4j.configuration=mylog4j.properties' \
  --morphline-file ../search-core/src/test/resources/test-morphlines/tutorialReadAvroContainer.conf \
  --solr-home-dir src/test/resources/solr/minimr \
  --output-dir hdfs://c2202.mycompany.com/user/$USER/test \
  --shard-url http://solr001.mycompany.com:8983/solr/collection1 \
  --shard-url http://solr002.mycompany.com:8983/solr/collection1 \
  --go-live \
  hdfs:///user/foo/indir

# Go live by merging resulting index shards into a live SolrCloud cluster
# (discover shards and Solr URLs through ZooKeeper):
sudo -u hdfs hadoop \
  --config /etc/hadoop/conf.cloudera.mapreduce1 \
  jar target/search-mr-*-job.jar org.apache.solr.hadoop.MapReduceIndexerTool \
  --libjars myconfig.jar \
  -D 'mapred.child.java.opts=-Xmx500m -Dlog4j.configuration=mylog4j.properties' \
  --morphline-file ../search-core/src/test/resources/test-morphlines/tutorialReadAvroContainer.conf \
  --output-dir hdfs://c2202.mycompany.com/user/$USER/test \
  --zk-host zk01.mycompany.com:2181/solr \
  --collection collection1 \
  --go-live \
  hdfs:///user/foo/indir
</pre>

## HdfsFindTool

HdfsFindTool is essentially the HDFS version of the Linux file system 'find' command. 
The command walks one or more HDFS directory trees and finds all HDFS files that match the specified expression and applies selected actions to them. 
By default, it simply prints the list of matching HDFS file paths to stdout, one path per line. 
The output file list can be piped into the MapReduceIndexerTool via the MapReduceIndexerTool --inputlist option. 
More details are available through the command line help:

<pre>
$ hadoop jar target/search-mr-*-job.jar org.apache.solr.hadoop.HdfsFindTool -help

Usage: hadoop fs [generic options]
  [-find <path> ... <expression> ...]
  [-help [cmd ...]]
  [-usage [cmd ...]]

-find <path> ... <expression> ...:  Finds all files that match the specified expression and applies selected actions to them.

    The following primary expressions are recognised:
      -atime n
      -amin n
        Evaluates as true if the file access time subtracted from
        the start time is n days (or minutes if -amin is used).

      -blocks n
        Evaluates to true if the number of file blocks is n.

      -class classname [args ...]
        Executes the named expression class.

      -depth
        Always evaluates to true. Causes directory contents to be
        evaluated before the directory itself.

      -empty
        Evaluates as true if the file is empty or directory has no
        contents.

      -exec command [argument ...]
      -ok command [argument ...]
        Executes the specified Hadoop shell command with the given
        arguments. If the string {} is given as an argument then
        is replaced by the current path name.  If a {} argument is
        followed by a + character then multiple paths will be
        batched up and passed to a single execution of the command.
        A maximum of 500 paths will be passed to a single
        command. The expression evaluates to true if the command
        returns success and false if it fails.
        If -ok is specified then confirmation of each command shall be
        prompted for on STDERR prior to execution.  If the response is
        'y' or 'yes' then the command shall be executed else the command
        shall not be invoked and the expression shall return false.

      -group groupname
        Evaluates as true if the file belongs to the specified
        group.

      -mtime n
      -mmin n
        Evaluates as true if the file modification time subtracted
        from the start time is n days (or minutes if -mmin is used)

      -name pattern
      -iname pattern
        Evaluates as true if the basename of the file matches the
        pattern using standard file system globbing.
        If -iname is used then the match is case insensitive.

      -newer file
        Evaluates as true if the modification time of the current
        file is more recent than the modification time of the
        specified file.

      -nogroup
        Evaluates as true if the file does not have a valid group.

      -nouser
        Evaluates as true if the file does not have a valid owner.

      -perm [-]mode
      -perm [-]onum
        Evaluates as true if the file permissions match that
        specified. If the hyphen is specified then the expression
        shall evaluate as true if at least the bits specified
        match, otherwise an exact match is required.
        The mode may be specified using either symbolic notation,
        eg 'u=rwx,g+x+w' or as an octal number.

      -print
      -print0
        Always evaluates to true. Causes the current pathname to be
        written to standard output. If the -print0 expression is
        used then an ASCII NULL character is appended.

      -prune
        Always evaluates to true. Causes the find command to not
        descend any further down this directory tree. Does not
        have any affect if the -depth expression is specified.

      -replicas n
        Evaluates to true if the number of file replicas is n.

      -size n[c]
        Evaluates to true if the file size in 512 byte blocks is n.
        If n is followed by the character 'c' then the size is in bytes.

      -type filetype
        Evaluates to true if the file type matches that specified.
        The following file type values are supported:
        'd' (directory), 'l' (symbolic link), 'f' (regular file).

      -user username
        Evaluates as true if the owner of the file matches the
        specified user.

    The following operators are recognised:
      expression -a expression
      expression -and expression
      expression expression
        Logical AND operator for joining two expressions. Returns
        true if both child expressions return true. Implied by the
        juxtaposition of two expressions and so does not need to be
        explicitly specified. The second expression will not be
        applied if the first fails.

      ! expression
      -not expression
        Evaluates as true if the expression evaluates as false and
        vice-versa.

      expression -o expression
      expression -or expression
        Logical OR operator for joining two expressions. Returns
        true if one of the child expressions returns true. The
        second expression will not be applied if the first returns
        true.

-help [cmd ...]:  Displays help for given command or all commands if none
    is specified.

-usage [cmd ...]: Displays the usage for given command or all commands if none
    is specified.

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
</pre>

Example: Find all files that match all of the following conditions
* File is contained somewhere in the directory tree hdfs:///user/$USER/solrloadtest/twitter/tweets
* file name matches the glob pattern 'sample-statuses*.gz'
* file was last modified less than 1440 minutes (i.e. 24 hours) ago
* file size is between 1 MB and 1 GB

<pre>
$ hadoop jar target/search-mr-*-job.jar org.apache.solr.hadoop.HdfsFindTool -find hdfs:///user/$USER/solrloadtest/twitter/tweets -type f -name 'sample-statuses*.gz' -mmin -1440 -size -1000000000c -size +1000000c
</pre>
