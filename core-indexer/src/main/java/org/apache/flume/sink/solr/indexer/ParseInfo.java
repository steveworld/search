/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.sink.solr.indexer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.solr.handler.extraction.SolrContentHandler;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;

/**
 * Conduit for passing state between parser and sink components in cases where
 * API calls or {@link ParseContext} don't permit direct parameter passing.
 * Basically, this class is a more practical alternative to {@link ParseContext}
 * .
 */
public final class ParseInfo {

  private final StreamEvent event;
  private final SolrIndexer indexer;
  private String id;
  private SolrCollection solrCollection;
  private SolrContentHandler solrContentHandler;
  private Metadata metadata;
  private final AtomicLong recordNumber = new AtomicLong();
  private boolean isMultiDocumentParser = false;
  private final Map<String, Object> params = new HashMap();

  public ParseInfo(StreamEvent event, SolrIndexer indexer) {
    if (event == null) {
      throw new IllegalArgumentException("Event must not be null");
    }
    if (indexer == null) {
      throw new IllegalArgumentException("Indexer must not be null");
    }
    this.event = event;
    this.indexer = indexer;
  }

  public StreamEvent getEvent() {
    return event;
  }

  public SolrIndexer getIndexer() {
    return indexer;
  }

  public Configuration getConfig() {
    return indexer.getConfig();
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public SolrCollection getSolrCollection() {
    return solrCollection;
  }

  public void setSolrCollection(SolrCollection solrCollection) {
    this.solrCollection = solrCollection;
  }

  public SolrContentHandler getSolrContentHandler() {
    return solrContentHandler;
  }

  public void setSolrContentHandler(SolrContentHandler solrContentHandler) {
    this.solrContentHandler = solrContentHandler;
  }

  public Metadata getMetadata() {
    return metadata;
  }

  public void setMetadata(Metadata metadata) {
    this.metadata = metadata;
  }

  public boolean isMultiDocumentParser() {
    return isMultiDocumentParser;
  }

  public void setMultiDocumentParser(boolean isMultiDocumentParser) {
    this.isMultiDocumentParser = isMultiDocumentParser;
  }

  public AtomicLong getRecordNumber() {
    return recordNumber;
  }

  public Map<String, Object> getParams() {
    return params;
  }

}
