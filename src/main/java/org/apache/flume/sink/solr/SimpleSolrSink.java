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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
//import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flume sink that extracts search documents from Apache Flume events,
 * transforms them and loads them into Apache Solr.
 */
// @InterfaceStability.Evolving
public class SimpleSolrSink extends AbstractSink implements Configurable {

  private Map<String, SolrCollection> solrCollections; // proxies to remote solr
  private Context context;
  private SimpleSolrSinkCounter solrSinkCounter; // TODO: replace with
                                                 // metrics.codahale.com

  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleSolrSink.class);

  public SimpleSolrSink() {
  }

  @Override
  public void configure(Context context) {
    this.context = context;
    if (solrSinkCounter == null) {
      solrSinkCounter = new SimpleSolrSinkCounter(getName());
    }
  }

  /** Returns the Flume configuration settings */
  protected Context getContext() {
    return context;
  }

  /**
   * Returns the Solr collection proxies to which this sink can route Solr
   * documents
   */
  protected final Map<String, SolrCollection> getSolrCollections() {
    return solrCollections;
  }

  /**
   * Creates the Solr collection proxies to which this sink can route Solr
   * documents; override to customize
   */
  protected Map<String, SolrCollection> createSolrCollections() {
    String solrServerUrl = "http://127.0.0.1:8983/solr/collection1";
    return Collections.singletonMap(solrServerUrl, new SolrCollection(solrServerUrl, new SolrServerDocumentLoader(
        new HttpSolrServer(solrServerUrl))));
  }

  /**
   * Returns the maximum number of events to take per flume transaction;
   * override to customize
   */
  protected int getMaxBatchSize() {
    return 1000;
  }

  /** Returns the maximum duration per flume transaction; override to customize */
  protected long getMaxBatchDurationMillis() {
    return 10 * 1000;
  }

  /**
   * Returns whether or not we shall log exceptions during cleanup attempts that
   * probably happened as a result of a prior exception; Override to customize.
   */
  protected boolean isLoggingSubsequentExceptions() {
    return false;
  }

  @Override
  public synchronized void start() {
    LOGGER.info("Starting sink {} ...", this);
    solrSinkCounter.start();
    if (solrCollections == null) {
      solrCollections = Collections.unmodifiableMap(new LinkedHashMap(createSolrCollections()));
    }
    super.start();
    LOGGER.info("Solr sink {} started.", getName());
  }

  @Override
  public synchronized void stop() {
    LOGGER.info("Solr sink {} stopping...", getName());
    try {
      for (SolrCollection collection : getSolrCollections().values()) {
        collection.shutdown();
      }
      solrSinkCounter.stop();
      LOGGER.info("Solr sink {} stopped. Metrics: {}, {}", getName(), solrSinkCounter);
    } finally {
      solrCollections = null;
      super.stop();
    }
  }

  @Override
  public synchronized Status process() throws EventDeliveryException {
    int batchSize = getMaxBatchSize();
    long batchEndTime = System.currentTimeMillis() + getMaxBatchDurationMillis();
    Channel myChannel = getChannel();
    Transaction txn = myChannel.getTransaction();
    txn.begin();
    boolean isSolrTransactionCommitted = true;
    try {
      int numEventsTaken = 0;
      beginSolrTransaction();
      isSolrTransactionCommitted = false;

      // repeatedly take and process events from the Flume queue
      for (int i = 0; i < batchSize; i++) {
        long startTime = System.nanoTime();
        Event event = myChannel.take();
        solrSinkCounter.addToTakeNanos(System.nanoTime() - startTime);
        if (event == null) {
          break;
        }
        numEventsTaken++;
        LOGGER.debug("solr event: {}", event);
        process(event);
        if (System.currentTimeMillis() >= batchEndTime) {
          break;
        }
      }

      // update metrics
      if (numEventsTaken == 0) {
        solrSinkCounter.incrementBatchEmptyCount();
      }
      if (numEventsTaken < batchSize) {
        solrSinkCounter.incrementBatchUnderflowCount();
      } else {
        solrSinkCounter.incrementBatchCompleteCount();
      }
      solrSinkCounter.addToEventDrainAttemptCount(numEventsTaken);
      solrSinkCounter.addToEventDrainSuccessCount(numEventsTaken);

      commitSolrTransaction();
      isSolrTransactionCommitted = true;
      txn.commit();
      return numEventsTaken == 0 ? Status.BACKOFF : Status.READY;
    } catch (Throwable t) {
      // Ooops - need to rollback and perhaps back off
      try {
        LOGGER.error("Solr Sink " + getName() + ": Unable to process event from channel " + myChannel.getName()
            + ". Exception follows.", t);
      } catch (Throwable t1) {
        ; // ignore logging error
      } finally {
        try {
          if (!isSolrTransactionCommitted) {
            rollbackSolrTransaction();
          }
        } catch (Throwable t2) {
          try {
            if (isLoggingSubsequentExceptions()) { // log and ignore
              LOGGER.warn("Cannot rollback solr transaction, and there was an exception prior to this exception: ", t2);
            }
          } catch (Throwable t3) {
            ; // ignore logging error
          }
        } finally {
          try {
            txn.rollback();
          } catch (Throwable t4) {
            try {
              if (isLoggingSubsequentExceptions()) { // log and ignore
                LOGGER.warn("Cannot rollback flume transaction, and there was an exception prior to this exception: ",
                    t4);
              }
            } catch (Throwable t5) {
              ; // ignore logging error
            }
          }
        }
      }

      if (t instanceof Error) {
        throw (Error) t; // rethrow original exception
      } else if (t instanceof ChannelException) {
        return Status.BACKOFF;
      } else {
        throw new EventDeliveryException("Failed to send events", t); // rethrow
      }
    } finally {
      txn.close();
    }
  }

  /** Extracts, transforms and loads the given Flume event into Solr */
  public void process(Event event) throws IOException, SolrServerException {
    // LOGGER.debug("threadId: {}", Thread.currentThread().getId());
    // TODO: use queue to support parallel ETL across multiple CPUs?
    long startTime = System.nanoTime();
    List<SolrInputDocument> docs = extract(event);
    solrSinkCounter.addToExtractNanos(System.nanoTime() - startTime);

    startTime = System.nanoTime();
    docs = transform(docs);
    solrSinkCounter.addToTransformNanos(System.nanoTime() - startTime);

    startTime = System.nanoTime();
    load(docs);
    solrSinkCounter.addToLoadNanos(System.nanoTime() - startTime);
  }

  /**
   * Extracts the given Flume event and maps it into zero or more Solr documents
   */
  protected List<SolrInputDocument> extract(Event event) {
    SolrInputDocument doc = new SolrInputDocument();
    for (Entry<String, String> entry : event.getHeaders().entrySet()) {
      doc.setField(entry.getKey(), entry.getValue());
    }
    if (event.getBody() != null) {
      doc.setField("body", event.getBody());
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
    getSolrCollections().get(collectionName).load(docs);
  }

  /** Begins a solr transaction */
  public void beginSolrTransaction() {
    for (SolrCollection collection : getSolrCollections().values()) {
      collection.beginTransaction();
    }
  }

  /**
   * Sends any outstanding documents to solr and waits for a positive or
   * negative ack (i.e. exception) from solr. Depending on the outcome the
   * caller should then commit or rollback the current flume transaction
   * correspondingly.
   */
  public void commitSolrTransaction() {
    for (SolrCollection collection : getSolrCollections().values()) {
      collection.commitTransaction();
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
  public void rollbackSolrTransaction() throws SolrServerException, IOException {
    for (SolrCollection collection : getSolrCollections().values()) {
      collection.rollback();
    }
  }

  @Override
  public String toString() {
    int i = getClass().getName().lastIndexOf('.') + 1;
    String shortClassName = getClass().getName().substring(i);
    return shortClassName + " " + getName();
  }

}
