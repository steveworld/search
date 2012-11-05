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
package org.apache.flume.sink.solr;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.DateUtil;
import org.apache.solr.schema.IndexSchema;

/**
 * A SolrServer with a schema and associated meta data representing a Solr Collection.
 */
public class SolrCollection {

  private final String name;
  private final SolrServer server;
  private IndexSchema schema;
  private SolrParams solrParams = new MapSolrParams(new HashMap());
  private Collection<String> dateFormats = DateUtil.DEFAULT_DATE_FORMATS;
  private long numLoadedDocs = 0; // number of documents loaded in the current transaction
  
  public SolrCollection(String name, SolrServer server) {
    if (name == null) {
      throw new IllegalArgumentException();
    }
    if (server == null) {
      throw new IllegalArgumentException();
    }
    this.name = name;
    this.server = server;
  }
  
  /** Begins a Solr transaction */
  public void beginSolrTransaction() {
    numLoadedDocs = 0;
    if (server instanceof SafeConcurrentUpdateSolrServer) {
      ((SafeConcurrentUpdateSolrServer) server).clearException();
    }
  }
  
  /** Loads the given documents into Solr */
  public void load(List<SolrInputDocument> docs) throws IOException, SolrServerException {
    if (docs.size() > 0) {
      numLoadedDocs += docs.size();
      UpdateResponse rsp = server.add(docs);
    }
  }

  /**
   * Sends any outstanding documents to Solr and waits for a positive or negative ack (i.e. exception) from solr.
   * Depending on the outcome the caller should then commit or rollback the current flume transaction correspondingly.
   */
  public void commitSolrTransaction() {
    if (numLoadedDocs > 0) {
      if (server instanceof ConcurrentUpdateSolrServer) {
        ((ConcurrentUpdateSolrServer) server).blockUntilFinished();
      }
    }
  }
  
  /** Releases allocated resources */
  public void shutdown() {
    server.shutdown();
  }
  
  public SolrServer getSolrServer() {
    return server;
  }
  
  public String getName() {
    return name;
  }

  public IndexSchema getSchema() {
    return this.schema;
  }

  public void setSchema(IndexSchema schema) {
    this.schema = schema;
  }
  
  public SolrParams getSolrParams() {
    return solrParams;
  }

  public void setSolrParams(SolrParams solrParams) {
    this.solrParams = solrParams;
  }

  public Collection<String> getDateFormats() {
    return dateFormats;
  }

  public void setDateFormats(Collection<String> dateFormats) {
    this.dateFormats = dateFormats;
  }
  
}
