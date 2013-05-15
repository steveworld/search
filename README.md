# Cloudera Search

The sub-directories contain:

## cdk-morphlines-core

Morphline runtime and standard library that higher level modules such as cdk-morphlines-avro and cdk-morphlines-tika depend on.

## cdk-morphlines-avro

Morphline commands for reading, extracting and transforming Avro files and Avro objects.

## cdk-morphlines-tika

Morphline commands for auto-detecting MIME types, as well as decompressing and unpacking files. Depends on Apache Tika.

## search-core

Morphline commands for Solr that higher level modules such as search-solrcell and search-mr and search-flume depend on for indexing.

## search-solrcell

Morphline commands for using SolrCell with Tika parsers.

## search-flume

Flume sink that extracts search documents from Apache Flume events (using a morphline), transforms them and loads them into Apache Solr.

## search-mr

Flexible, scalable, fault tolerant, batch oriented system for processing large numbers of records contained in files that are stored on HDFS into search indexes stored on HDFS; Uses a morphline.

## search-contrib

Additional sources to help with search.

## samples

Example configurations and test data files.


# Building

This step builds the software from source.

<pre>
git clone git@github.com:cloudera/search.git
cd search
#git checkout master
mvn clean package
find . -name '*.jar'
</pre>

# How to integrate the codeline with Eclipse

* Build the software as described above. Then create Eclipse projects like this:
<pre>
cd search
mvn test -DskipTests eclipse:eclipse
</pre>
* mvn eclipse:eclipse creates several Eclipse projects, one for each mvn submodule. It will also download and attach the jars of all transitive dependencies and their source code to the eclipse projects, so you can readily browse around the source of the entire call stack.
* Then in eclipse do Menu File/Import/Maven/Existing Maven Project/ on the root parent directory ~/search and select all submodules, then "Next" and "Finish". 
* You will see some maven project errors that keep eclipse from building the workspace b/c the eclipse maven plugin has some weird quirks and limitations. To work around this, next, disable the mvn "Nature" by clicking on the project in the browser, right clicking on Menu Maven/Disable Maven Nature. This way you get all the niceties of the maven dependency management without the hassle of the (current) maven eclipse plugin, everything compiles fine from within Eclipse, and junit works and passes from within Eclipse as well. 
* When a pom changes simply rerun mvn eclipse:eclipse and then run Menu Eclipse/Refresh Project. No need to disable the Maven "Nature" again and again.
* To run junit tests from within eclipse click on the project (e.g. search-core or search-mr, etc) in the eclipse project explorer, right click, Run As/JUnit Test, and, for search-mr, makes sure to give it the following VM arguments: -ea -Xmx512m -XX:MaxDirectMemorySize=256m -XX:MaxPermSize=128M