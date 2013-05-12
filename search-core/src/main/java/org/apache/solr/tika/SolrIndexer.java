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
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.tika.exception.TikaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.google.common.base.Joiner;
import com.typesafe.config.Config;
import com.codahale.metrics.MetricRegistry;

/**
 * Indexer that extracts search documents from events, transforms them and loads them into Apache
 * Solr.
 */
public class SolrIndexer {

  private final Config config;
  private SolrCollection solrCollection; // proxy to remote solr  
  private final boolean ignoreLoads; // for load testing only
  private final MetricRegistry metricRegistry;

  /**
   * Some exceptions tend to be transient, in which case the corresponding task can be retried.
   * Example: network connection errors, timeouts, etc. These are called recoverable exceptions.
   * 
   * In contrast, the task associated with an unrecoverable exception cannot succeed on retry.
   * Examples: Corrupt or malformed parser input data. Parser bugs. Unknown Solr schema field.
   * 
   * In production mode (configuration parameter isProductionMode=true) we log and ignore
   * unrecoverable exceptions. This enables mission critical large scale online production systems
   * to make progress despite some issues.
   * 
   * In non-production mode (aka test mode, configuration parameter isProductionMode=false) we throw
   * exceptions up the call chain in order to fail fast and provide better debugging diagnostics to
   * the user.
   * 
   * The default is non-production mode (aka test mode).
   * 
   * It is not inconceivable that there might be a bug where an unrecoverable exception is
   * misclassified as recoverable. Nonetheless, in production we need a way to avoid retrying that
   * event forever, and we need to ensure we can make progress. Thus, in production mode we also log
   * and ignore recoverable exceptions if the configuration parameter
   * ignoreRecoverableExceptions=true. By default we have ignoreRecoverableExceptions=false. This
   * flag should only be enabled if a misclassification bug has been identified. Please report such
   * a bug to Cloudera.
   * 
   * In case we do throw an exception up per the rules described above, the caller can catch the
   * exception and decide to retry the task if he sees fit.
   * 
   * Example MapReduce Usage:
   * 
   * hadoop ... -D org.apache.solr.tika.SolrIndexer.isProductionMode=true -D
   * org.apache.solr.tika.SolrIndexer.ignoreRecoverableExceptions=true
   * 
   * Example Flume Usage in flume.conf:
   * 
   * agent.sinks.solrSink.org.apache.solr.tika.SolrIndexer.isProductionMode = true
   * agent.sinks.solrSink.org.apache.solr.tika.SolrIndexer. ignoreRecoverableExceptions = true
   */
  public static final String PRODUCTION_MODE = SolrIndexer.class.getName() + ".isProductionMode"; // ExtractingParams.IGNORE_TIKA_EXCEPTION;
  public static final String IGNORE_RECOVERABLE_EXCEPTIONS = SolrIndexer.class.getName() + ".ignoreRecoverableExceptions";

  /**
   * If true this boolean configuration parameter simulates an infinitely fast pipe into Solr for
   * load testing. This can be used to easily isolate performance metrics of the extraction and
   * transform phase.
   */
  private static final String IGNORE_LOADS = SolrIndexer.class.getName() + ".ignoreLoads";
  
  private static final Logger LOGGER = LoggerFactory.getLogger(SolrIndexer.class);

  public SolrIndexer(SolrCollection solrCollection, Config config, MetricRegistry metricRegistry) {
    if (solrCollection == null) {
      throw new IllegalArgumentException("solrCollection must not be null");
    }
    if (config == null) {
      throw new IllegalArgumentException("Config must not be null");
    }
    if (metricRegistry == null) {
      throw new IllegalArgumentException("metricRegistry must not be null");
    }
    this.config = config;
    this.solrCollection = solrCollection;
    this.ignoreLoads = config.hasPath(IGNORE_LOADS) && config.getBoolean(IGNORE_LOADS);
    this.metricRegistry = metricRegistry;
    LOGGER.info("Number of solr schema fields: {}", solrCollection.getSchema().getFields().size());
    LOGGER.info("Solr schema: \n{}", Joiner.on("\n").join(new TreeMap(solrCollection.getSchema().getFields()).values()));
  }
  
  /**
   * Returns the Solr collection proxy to which this indexer can route Solr documents
   */
  public final SolrCollection getSolrCollection() {
    return solrCollection;
  }

  public synchronized void stop() {
    try {
      solrCollection.getDocumentLoader().shutdown();
    } finally {
      solrCollection = null;
    }
  }

  /** Extracts, transforms and loads the given event into Solr */
  public void process(StreamEvent event) throws IOException, SolrServerException, SAXException, TikaException {
    List<SolrInputDocument> docs = extract(event);
    docs = transform(docs);
    load(docs);
  }

  /**
   * Extracts the given event and maps it into zero or more Solr documents
   */
  @SuppressWarnings("unused")
  protected List<SolrInputDocument> extract(StreamEvent event) throws IOException, SolrServerException, SAXException, TikaException {
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
   * Extension point to transform a list of documents in an application specific way. Does nothing
   * by default
   */
  protected List<SolrInputDocument> transform(List<SolrInputDocument> docs) {
    return docs;
  }

  /** Loads the given documents into Solr */
  public void load(List<SolrInputDocument> docs) throws IOException, SolrServerException {
    if (!ignoreLoads) {
      solrCollection.getDocumentLoader().load(docs);
    }
  }

  /** Begins a solr transaction */
  public void beginTransaction() {
    solrCollection.getDocumentLoader().beginTransaction();
  }

  /**
   * Sends any outstanding documents to solr and waits for a positive or negative ack (i.e.
   * exception) from solr. Depending on the outcome the caller should then commit or rollback the
   * outer (flume) transaction correspondingly.
   */
  public void commitTransaction() throws SolrServerException, IOException {
    solrCollection.getDocumentLoader().commitTransaction();
  }

  /**
   * Performs a rollback of all non-committed documents pending.
   * <p>
   * Note that this is not a true rollback as in databases. Content you have previously added may
   * have already been committed due to autoCommit, buffer full, other client performing a commit
   * etc. So this is only a best-effort rollback, not a rollback in a strict 2PC protocol.
   * 
   * @throws IOException
   *           If there is a low-level I/O error.
   */
  public void rollback() throws SolrServerException, IOException {
    solrCollection.getDocumentLoader().rollback();
  }

  /** Returns the configuration settings */
  protected Config getConfig() {
      return config;
  }
  
  protected boolean isProductionMode() {
    boolean isProductionMode = false;
    if (getConfig().hasPath(PRODUCTION_MODE)) { 
      isProductionMode = getConfig().getBoolean(PRODUCTION_MODE);
    }
    return isProductionMode;
  }

  protected boolean isIgnoringRecoverableExceptions() {
    boolean ignoreRecoverableExceptions = false;
    if (getConfig().hasPath(IGNORE_RECOVERABLE_EXCEPTIONS)) { 
      ignoreRecoverableExceptions = getConfig().getBoolean(IGNORE_RECOVERABLE_EXCEPTIONS);
    }
    return ignoreRecoverableExceptions;
  }
  
  protected boolean isRecoverableException(Throwable t) {
    return RecoverableSolrException.isRecoverable(t);
  }

  protected void handleException(Throwable t, StreamEvent event) throws IOException, SolrServerException, SAXException, TikaException {
    if (t instanceof Error) {
      throw (Error) t; // never ignore errors
    }
    if (isProductionMode()) {
      if (!isRecoverableException(t)) {
        LOGGER.warn("Ignoring unrecoverable exception in production mode for event: " + event, t);
        return;
      } else if (isIgnoringRecoverableExceptions()) {
        LOGGER.warn("Ignoring recoverable exception in production mode for event: " + event, t);
        return;
      }
    }
    if (t instanceof IOException) {
      throw (IOException) t;
    } else if (t instanceof SolrServerException) {
      throw (SolrServerException) t;
    } else if (t instanceof SAXException) {
      throw (SAXException) t;
    } else if (t instanceof TikaException) {
      throw (TikaException) t;
    } else if (t instanceof RuntimeException) {
      throw (RuntimeException) t;
    } else {
      throw new IndexerException(t);
    }
  }

  /**
   * Get the MetricRegistry
   */
  public MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }
}
