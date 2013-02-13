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
package org.apache.solr.tika;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.typesafe.config.Config;

/**
 * Indexer that extracts search documents from events, transforms them and
 * loads them into Apache Solr.
 */
public class SolrIndexer {

  private String name;
  private Configuration config;
  private Map<String, SolrCollection> solrCollections; // proxies to remote solr

  private static final Logger LOGGER = LoggerFactory.getLogger(SolrIndexer.class);

  public SolrIndexer() {
  }
  
  public void configure(Config config) {
    this.config = new Configuration(config);
  }
  
  /** Returns the configuration settings */
  protected Configuration getConfig() {
      return config;
  }

  public String getName() {
    return name;
  }
  
  public synchronized void setName(String name) {
    this.name = name;
  }

  /**
   * Returns the Solr collection proxies to which this indexer can route Solr
   * documents
   */
  public final Map<String, SolrCollection> getSolrCollections() {
    return solrCollections;
  }

  /**
   * Creates the Solr collection proxies to which this indexer can route Solr
   * documents; override to customize
   */
  protected Map<String, SolrCollection> createSolrCollections() {
    String solrServerUrl = "http://127.0.0.1:8983/solr/collection1";
    return Collections.singletonMap(solrServerUrl, new SolrCollection(solrServerUrl, new SolrServerDocumentLoader(
        new HttpSolrServer(solrServerUrl))));
  }

  public synchronized void start() {
    LOGGER.info("Starting indexer {} ...", this);
    if (solrCollections == null) {
      solrCollections = Collections.unmodifiableMap(new LinkedHashMap(createSolrCollections()));
    }
    LOGGER.info("Indexer {} started.", getName());
    for (SolrCollection collection : getSolrCollections().values()) {
      LOGGER.info("Number of solr schema fields: {}", collection.getSchema().getFields().size());
      LOGGER.info("Solr schema: \n{}", Joiner.on("\n").join(new TreeMap(collection.getSchema().getFields()).values()));
    }
  }

  public synchronized void stop() {
    LOGGER.info("Indexer {} stopping...", getName());
    try {
      for (SolrCollection collection : getSolrCollections().values()) {
        collection.getDocumentLoader().shutdown();
      }
      LOGGER.info("Indexer {} stopped.", getName());
    } finally {
      solrCollections = null;
    }
  }

  /** Extracts, transforms and loads the given event into Solr */
  public void process(StreamEvent event) throws IOException, SolrServerException {
    List<SolrInputDocument> docs = extract(event);
    docs = transform(docs);
    load(docs);
  }

  /**
   * Extracts the given event and maps it into zero or more Solr documents
   */
  protected List<SolrInputDocument> extract(StreamEvent event) {
    SolrInputDocument doc = new SolrInputDocument();
    for (Entry<String, String> entry : event.getHeaders().entrySet()) {
      doc.setField(entry.getKey(), entry.getValue());
    }
    InputStream in = event.getBody();
    if (in != null) {
      try {
        byte[] bytes = IOUtils.toByteArray(in);
        doc.setField("body", bytes);
      } catch (IOException e) {
        throw new IndexerException(e);
      } finally {
        IOUtils.closeQuietly(in);
      }
    }
    return Collections.singletonList(doc);
  }

  /**
   * Extension point to transform a list of documents in an application specific
   * way. Does nothing by default
   */
  protected List<SolrInputDocument> transform(List<SolrInputDocument> docs) {
    return docs;
  }

  /** Loads the given documents into Solr */
  public void load(List<SolrInputDocument> docs) throws IOException, SolrServerException {
    for (String collectionName : getSolrCollections().keySet()) {
      load(docs, collectionName);
    }
  }

  /** Loads the given documents into the specified Solr collection */
  public void load(List<SolrInputDocument> docs, String collectionName) throws IOException, SolrServerException {
    getSolrCollections().get(collectionName).getDocumentLoader().load(docs);
  }

  /** Begins a solr transaction */
  public void beginTransaction() {
    for (SolrCollection collection : getSolrCollections().values()) {
      collection.getDocumentLoader().beginTransaction();
    }
  }

  /**
   * Sends any outstanding documents to solr and waits for a positive or
   * negative ack (i.e. exception) from solr. Depending on the outcome the
   * caller should then commit or rollback the outer (flume) transaction
   * correspondingly.
   */
  public void commitTransaction() {
    for (SolrCollection collection : getSolrCollections().values()) {
      collection.getDocumentLoader().commitTransaction();
    }
  }

  /**
   * Performs a rollback of all non-committed documents pending.
   * <p>
   * Note that this is not a true rollback as in databases. Content you have
   * previously added may have already been committed due to autoCommit, buffer
   * full, other client performing a commit etc. So this is only a best-effort
   * rollback, not a rollback in a strict 2PC protocol.
   * 
   * @throws IOException
   *           If there is a low-level I/O error.
   */
  public void rollback() throws SolrServerException, IOException {
    for (SolrCollection collection : getSolrCollections().values()) {
      collection.getDocumentLoader().rollback();
    }
  }

  @Override
  public String toString() {
    int i = getClass().getName().lastIndexOf('.') + 1;
    String shortClassName = getClass().getName().substring(i);
    return getName() + " (" + shortClassName + ")";
  }

}
