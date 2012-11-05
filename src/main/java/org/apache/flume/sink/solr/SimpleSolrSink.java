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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flume sink that extracts search documents from Apache Flume events, transforms them and loads them into
 * Apache Solr.
 */
public class SimpleSolrSink extends AbstractSink implements Configurable {
  
  private Map<String, SolrCollection> solrCollections; // proxies to remote solr
  private Context context;
  private CountDownLatch isStopping = new CountDownLatch(1); // indicates we should shutdown ASAP. TODO: unnecessary? let's ask flumers
  private CountDownLatch isStopped = new CountDownLatch(1); // indicates we are stopped. TODO: unnecessary? let's ask flumers
  private SimpleSolrSinkCounter solrSinkCounter; // TODO: replace with http://metrics.codahale.com
  
  private static final AtomicLong SEQ_NUM = new AtomicLong();
  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleSolrSink.class);

  public SimpleSolrSink() {
  }
  
  @Override
  public void configure(Context context) {
    this.context = context;
    if (solrSinkCounter == null) {
      solrSinkCounter = new SimpleSolrSinkCounter("" + getName() + "#" + SEQ_NUM.getAndIncrement());
    }
  }
  
  /** Returns the Flume configuration settings */
  protected Context getContext() {
    return context;
  }

  /** Returns the Solr collection proxies to which this sink can route Solr documents */
  protected final Map<String, SolrCollection> getSolrCollections() {
    return solrCollections;
  }
  
  /** Creates the Solr collection proxies to which this sink can route Solr documents; override to customize */
  protected Map<String, SolrCollection> createSolrCollections() {
    String solrServerUrl = "http://127.0.0.1:8983/solr/collection1";
    return Collections.singletonMap(solrServerUrl, new SolrCollection(solrServerUrl, new HttpSolrServer(solrServerUrl)));
  }
  
  /** Returns the maximum number of events to take per flume transaction; override to customize */
  protected int getMaxBatchSize() {
    return 1000;
  }
  
  /** Returns the maximum duration per flume transaction; override to customize */
  protected long getMaxBatchDurationMillis() {
    return 10 * 1000;
  }

  @Override
  public synchronized void start() {
    LOGGER.info("Starting sink {} ...", this);
    solrSinkCounter.start();
    isStopping = new CountDownLatch(1);
    isStopped = new CountDownLatch(1);
    if (solrCollections == null) {
      solrCollections = Collections.unmodifiableMap(new LinkedHashMap(createSolrCollections()));
    }
    super.start();
    LOGGER.info("Solr sink {} started.", getName());
  }

  @Override
  public synchronized void stop() {
    stop(15, TimeUnit.SECONDS);
  }
  
  /* start() and stop() are called from an arbitrary async Flume management thread */
  public synchronized void stop(long timeout, TimeUnit timeunit) {
    LOGGER.info("Solr sink {} stopping...", getName());
    isStopping.countDown(); // signal other thread that it should exit process() ASAP
    try {
      if (!isStopped.await(timeout, timeunit)) { // give other thread some time to exit process() gracefully
        if (timeout != 0) {
          LOGGER.warn("Failed to stop gracefully. Now shutting down anyway.");
        }
      }
    } catch (InterruptedException e) {
      throw new FlumeException(e);
    }
    
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
  public Status process() throws EventDeliveryException {    
    Channel ch = getChannel();
    Transaction tx = ch.getTransaction();
    try {
      int numEventsTaken = 0;
      tx.begin();
      beginSolrTransaction();
      long batchEndTime = System.currentTimeMillis() + getMaxBatchDurationMillis();
      int batchSize = getMaxBatchSize();
      for (int i = 0; i < batchSize; i++) { // repeatedly take and process events from the Flume queue
        synchronized (this) { // are we asked to return control ASAP?
          if (isStopping.await(0, TimeUnit.NANOSECONDS) || isStopped.await(0, TimeUnit.NANOSECONDS)) {
            break;
          }
        }
        long startTime = System.nanoTime();
        Event event = ch.take();
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
      
      commitSolrTransaction();
      tx.commit();
      solrSinkCounter.addToEventDrainSuccessCount(numEventsTaken);
      return numEventsTaken == 0 ? Status.BACKOFF : Status.READY;
    } catch (Throwable t) {
      tx.rollback();
      if (t instanceof Error) {
        throw (Error) t;
      } else if (t instanceof ChannelException) {
        LOGGER.error("Solr Sink " + getName() + ": Unable to get event from" +
            " channel " + ch.getName() + ". Exception follows.", t);
        return Status.BACKOFF;
      } else {
        throw new EventDeliveryException("Failed to send events", t);
      }
    } finally {
      tx.close();
      synchronized (this) { 
        try {
          if (isStopping.await(0, TimeUnit.NANOSECONDS)) { // are we asked to return control ASAP?
            isStopped.countDown(); // signal to other thread that we're done
          }
        } catch (InterruptedException e) {
          ; // ignore
        }
      }
    }        
  }

  /** Extracts, transforms and loads the given Flume event into Solr */
  public void process(Event event) throws IOException, SolrServerException {
//    LOGGER.debug("threadId: {}", Thread.currentThread().getId());    
    long startTime = System.nanoTime();
    List<SolrInputDocument> docs = extract(event); // TODO: use queue to support parallel ETL across multiple CPUs?
    solrSinkCounter.addToExtractNanos(System.nanoTime() - startTime);
    
    startTime = System.nanoTime();
    docs = transform(docs);
    solrSinkCounter.addToTransformNanos(System.nanoTime() - startTime);

    startTime = System.nanoTime();
    load(docs);
    solrSinkCounter.addToLoadNanos(System.nanoTime() - startTime);
  }

  /** Extracts the given Flume event and maps it into zero or more Solr documents */
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

  /** Extension point to transform a list of documents in an application specific way. Does nothing by default */
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
      collection.beginSolrTransaction();
    }
  }
  
  /**
   * Sends any outstanding documents to solr and waits for a positive or negative ack (i.e. exception) from solr.
   * Depending on the outcome the caller should then commit or rollback the current flume transaction correspondingly.
   */
  public void commitSolrTransaction() {
    for (SolrCollection collection : getSolrCollections().values()) {
      collection.commitSolrTransaction();
    }
  }
  
  @Override
  public String toString() {
    String shortClassName = getClass().getName().substring(getClass().getName().lastIndexOf('.') + 1);
    return shortClassName + " " + getName(); // + " { solrServers: " + getSolrServers() + " }";
  }
  
}
