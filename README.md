# Cloudera Search

Cloudera Search is [Apache Solr](http://lucene.apache.org/solr/) integrated with CDH, 
including Apache Lucene, Apache SolrCloud, Apache Flume, Apache Hadoop MapReduce & HDFS, 
and Apache Tika. Cloudera Search also includes integrations that make searching 
more scalable, easy to use, and optimized for both near-real-time and batch-oriented indexing.

## Maven Modules

The following maven modules currently exist:

### cdk-morphlines

Morphlines is an open source framework that reduces the time and skills necessary to build or 
change Search indexing applications. A morphline is a rich configuration file that makes it easy 
to define an ETL transformation chain that consumes any kind of data from any kind of data source,
processes the data and loads the results into Cloudera Search. Executing in a small embeddable 
Java runtime system, morphlines can be used for Near Real Time applications as well as Batch 
processing applications. 

Morphlines are easy to use, configurable and extensible, efficient and powerful. They can see been
as an evolution of Unix pipelines, generalised to work with streams of generic records and to be 
embedded into Hadoop components such as Search, Flume, MapReduce, Pig, Hive, Sqoop. 

The system ships with a set of frequently used high level transformation and I/O commands that can 
be combined in application specific ways. The plugin system allows to add new transformations and 
I/O commands and integrate existing functionality and third party systems in a straightforward 
manner. 

This enables rapid prototyping of Hadoop ETL applications, complex stream and event 
processing in real time, flexible Log File Analysis, integration of multiple heterogeneous input 
schemas and file formats, as well as reuse of ETL logic building blocks across Search applications.

Cloudera ships a high performance runtime that compiles a morphline on the fly and processes all 
commands of a given morphline in the same thread, and adds no artificial overheads. 
For high scalability, a large number of morphline instances can be deployed on a cluster in a 
large number of Flume agents and MapReduce tasks. 

### cdk-morphlines-core

This module contains the morphline compiler, runtime and standard library of commands that higher
level modules such as `cdk-morphlines-avro` and `cdk-morphlines-tika` depend on.

Includes commands for flexible log file analysis, single-line records, multi-line records, 
CSV files, regular expression based pattern matching and extraction, operations on fields for 
assignment and comparison, operations on fields with list and set semantics, if-then-else 
conditionals, string and timestamp conversions, scripting support for dynamic java code, 
a small rules engine, logging, metrics & counters, etc.

### cdk-morphlines-avro

Morphline commands for reading, extracting and transforming Avro files and Avro objects.

### cdk-morphlines-tika

Morphline commands for auto-detecting MIME types from binary data, as well as decompressing and 
unpacking files. Depends on Apache Tika.

### search-core

Morphline commands for Solr that higher level modules such as `search-solrcell` and `search-mr` 
and `search-flume` depend on for indexing.

### search-solrcell

Morphline commands for using SolrCell with Tika parsers. This includes support for HTML, 
XML, PDF, Word, Excel, Images, Audio, Video, etc. 

### search-flume

Flume Morphline Solr Sink that extracts search documents from Apache Flume events, transforms 
them and loads them in Near Real Time into Apache Solr, typically a SolrCloud.

Also includes a Flume MorphlineInterceptor that can be used to implement software defined network
routing policies in a Flume network topology, or to ignore certain events or alter or insert 
certain event headers via regular expression based pattern matching, etc.

### search-mr

Flexible, scalable, fault tolerant, batch oriented system for processing large numbers of records 
contained in files that are stored on HDFS into search indexes stored on HDFS.

`MapReduceIndexerTool` is a MapReduce batch job driver that takes a morphline and creates a set of Solr 
index shards from a set of input files and writes the indexes into HDFS in a flexible, scalable, 
and fault-tolerant manner. It also supports merging the output shards into a set of live 
customer-facing Solr servers, typically a SolrCloud.

### search-contrib

Additional sources to help with search.

### samples

Example configurations and test data files.


## Mailing List

* [Posting](mailto:search-user@cloudera.org)
* [Archive](http://groups.google.com/a/cloudera.org/group/search-user)
* [Subscribe and Unsubcribe](http://groups.google.com/a/cloudera.org/group/search-user/subscribe)

## Documentation

* [FAQ](http://www.cloudera.com)
* [Admin Guide](http://www.cloudera.com)
* [Release Notes](http://www.cloudera.com)

## License

Cloudera Search is provided under the Apache Software License 2.0. See the file
`LICENSE.txt` for more information.

## Building

This step builds the software from source.

<pre>
git clone git@github.com:cloudera/search.git
cd search
#git checkout master
mvn clean package
find . -name '*.jar'
</pre>

## Integrating with Eclipse

* This section describes how to integrate the codeline with Eclipse.
* Build the software as described above. Then create Eclipse projects like this:
<pre>
cd search
mvn test -DskipTests eclipse:eclipse
</pre>
* `mvn test -DskipTests eclipse:eclipse` creates several Eclipse projects, one for each maven submodule. 
It will also download and attach the jars of all transitive dependencies and their source code to the eclipse 
projects, so you can readily browse around the source of the entire call stack.
* Then in eclipse do Menu `File/Import/Maven/Existing Maven Project/` on the root parent 
directory `~/search` and select all submodules, then "Next" and "Finish". 
* You will see some maven project errors that keep eclipse from building the workspace because 
the eclipse maven plugin has some weird quirks and limitations. To work around this, next, disable 
the maven "Nature" by clicking on the project in the browser, right clicking on Menu 
`Maven/Disable Maven Nature`. This way you get all the niceties of the maven dependency management 
without the hassle of the (current) maven eclipse plugin, everything compiles fine from within 
Eclipse, and junit works and passes from within Eclipse as well. 
* When a pom changes simply rerun `mvn test -DskipTests eclipse:eclipse` and 
then run Menu `Eclipse/Refresh Project`. No need to disable the Maven "Nature" again and again.
* To run junit tests from within eclipse click on the project (e.g. `search-core` or `search-mr`, etc)
in the eclipse project explorer, right click, `Run As/JUnit Test`, and, for `search-mr`, additionally 
make sure to give it the following VM arguments: 
<pre>
-ea -Xmx512m -XX:MaxDirectMemorySize=256m -XX:MaxPermSize=128M
</pre>
