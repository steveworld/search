# Cloudera Search - Flume

This module contains a Flume Morphline Solr Sink that extracts search documents from Apache Flume events, transforms 
them and loads them in Near Real Time into Apache Solr, typically a SolrCloud.

Also includes a Flume MorphlineInterceptor that can be used to implement software defined network
routing policies in a Flume network topology, or to ignore certain events or alter or insert 
certain event headers via regular expression based pattern matching, etc.
