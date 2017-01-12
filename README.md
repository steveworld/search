# Cloudera Search

Cloudera Search is [köpa valtrex på nätet](http://www.herpesvirus.se/valtrex/) integrated with CDH,
including Apache Lucene, Apache SolrCloud, Apache Flume, Apache Hadoop MapReduce & HDFS,
and Apache Tika. Cloudera Search also includes integrations that make searching
more scalable, easy to use, and optimized for both near-real-time and batch-oriented indexing.

## Maven Modules

The following maven modules currently exist:

### cdk-morphlines

Cloudera Morphlines is an open source framework that reduces the time and skills necessary to build or
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

Currently there are three components that execute morphlines:

* MapReduceIndexerTool
* Flume Morphline Solr Sink
* Flume MorphlineInterceptor

Morphlines manipulate continuous or arbitrarily large streams of records. The data model can be
described as follows: A record is a set of named fields where each field has an ordered list of one
or more values. A value can be any Java Object. That is, a record is essentially a hash table where
each hash table entry contains a String key and a list of Java Objects as values. (The
implementation uses Guava’s ArrayListMultimap, which is a ListMultimap). Note that a field can have
multiple values and any two records need not use common field names. This flexible data model
corresponds exactly to the characteristics of the Solr/Lucene data model, meaning a record can be
seen as a SolrInputDocument. A field with zero values is removed from the record - fields with zero
values effectively do not exist.

Not only structured data, but also arbitrary binary data can be passed into and processed by a
morphline. By convention, a record can contain an optional field named _attachment_body, which can
be a Java java.io.InputStream or Java byte[]. Optionally, such binary input data can be
characterized in more detail by setting the fields named _attachment_mimetype (e.g. application/pdf)
and _attachment_charset (e.g. UTF-8) and _attachment_name (e.g. cars.pdf), which assists in
detecting and parsing the data type.

This generic data model is useful to support a wide range of applications.

A command transforms a record into zero or more records. Commands can access all record fields. For
example, commands can parse fields, set fields, remove fields, rename fields, find and replace
values, split a field into multiple fields, split a field into multiple values, or drop records.
Often, regular expression based pattern matching is used as part of the process of acting on fields.
The output records of a command are passed to the next command in the chain. A command has a Boolean
return code, indicating success or failure.

For example, consider the case of a multi-line input record: A command could take this multi-line
input record and divide the single record into multiple output records, one for each line. This
output could then later be further divided using regular expression commands, splitting each single
line record out into multiple fields in application specific ways.

A command can extract, clean, transform, join, integrate, enrich and decorate records in many other
ways. For example, a command can join records with external data sources such as relational
databases, key-value stores, local files or IP Geo lookup tables. It can also perform DNS
resolution, expand shortened URLs, fetch linked metadata from social networks, perform sentiment
analysis and annotate the record accordingly, continuously maintain statistics for analytics over
sliding windows, compute exact or approximate distinct values and quantiles, etc.

A command can also consume records and pass them to external systems. For example, a command can
load records into Solr or write them to a MapReduce Reducer or pass them into an online
dashboard.

A command can contain nested commands. Thus, a morphline is a tree of commands, akin to a push-based
data flow engine or operator tree in DBMS query execution engines.

A morphline has no notion of persistence or durability or distributed computing or node failover. It
is basically just a chain of in-memory transformations in the current thread. There is no need for a
morphline to manage multiple processes or nodes or threads because this is already covered by host
systems such as MapReduce, Flume, Storm, etc. However, a morphline does support passing
notifications on the control plane to command subtrees. Such notifications include
BEGIN_TRANSACTION, COMMIT_TRANSACTION, ROLLBACK_TRANSACTION, SHUTDOWN.

