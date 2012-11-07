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

import org.apache.solr.client.solrj.SolrServerException;
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
  private final DocumentLoader loader;
  private IndexSchema schema;
  private SolrParams solrParams = new MapSolrParams(new HashMap());
  private Collection<String> dateFormats = DateUtil.DEFAULT_DATE_FORMATS;
  
  public SolrCollection(String name, DocumentLoader loader) {
    if (name == null) {
      throw new IllegalArgumentException();
    }
    if (loader == null) {
      throw new IllegalArgumentException();
    }
    this.name = name;
    this.loader = loader;
  }
  
  /** Begins a Solr transaction */
  public void beginTransaction() {
    loader.beginTransaction();
  }
  
  /** Loads the given documents into Solr */
  public void load(List<SolrInputDocument> docs) throws IOException, SolrServerException {
    loader.load(docs);
  }

  /**
   * Sends any outstanding documents to Solr and waits for a positive or negative ack (i.e. exception) from solr.
   * Depending on the outcome the caller should then commit or rollback the current flume transaction correspondingly.
   */
  public void commitTransaction() {
    loader.commitTransaction();
  }
  
  /**
   * Performs a rollback of all non-committed documents pending.
   * <p>
   * Note that this is not a true rollback as in databases. Content you have previously
   * added may have already been committed due to autoCommit, buffer full, other client performing
   * a commit etc. So this is only a best-effort rollback, not a rollback in a strict 2PC protocol.
   * @throws IOException If there is a low-level I/O error.
   */
  public void rollback() throws SolrServerException, IOException {
    loader.rollback();
  }
  
  /** Releases allocated resources */
  public void shutdown() {
    loader.shutdown();
  }
  
  public DocumentLoader getDocumentLoader() {
    return loader;
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
