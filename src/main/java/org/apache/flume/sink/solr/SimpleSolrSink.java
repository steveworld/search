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
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flume sink that extracts search documents from Apache Flume events, transforms them and loads them into
 * Apache Solr.
 */
public class SimpleSolrSink extends AbstractSink implements Configurable {
  
  private SolrServer server; // proxy to remote solr
  private CountDownLatch isStopping = new CountDownLatch(1); // indicates we should shutdown ASAP
  private CountDownLatch isStopped = new CountDownLatch(1); // indicates we are stopped
  private int numLoadedDocs = 0; // number of documents loaded in the current transaction
      
  private final AtomicLong takeTimer = new AtomicLong(); // TODO: use codahale-metrics or org.apache.flume.CounterGroup?
  private final AtomicLong extractTimer = new AtomicLong();
  private final AtomicLong transformTimer = new AtomicLong();
  private final AtomicLong loadTimer = new AtomicLong();
  
  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleSolrSink.class);

  public SimpleSolrSink() {
    this(null);
  }
  
  /** For testing only */
  protected SimpleSolrSink(SolrServer server) {
    this.server = server;
  }
  
  @Override
  public void configure(Context context) {
  }

  @Override
  public synchronized void start() {
    isStopping = new CountDownLatch(1);
    isStopped = new CountDownLatch(1);
    if (server == null) {
      server = createSolrServer();
    }
    super.start();
  }

  protected SolrServer getSolrServer() {
    return server;
  }
  
  protected SolrServer createSolrServer() {
    String solrServerUrl = "http://127.0.0.1:8983/solr/collection1";
    return new HttpSolrServer(solrServerUrl);
  }
  
  @Override
  public synchronized void stop() {
    stop(60, TimeUnit.SECONDS);
  }
  
  /* start() and stop() are called from an arbitrary async Flume management thread */
  public synchronized void stop(long timeout, TimeUnit timeunit) {
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
      SolrServer s = getSolrServer();
      if (s != null && (!(s instanceof EmbeddedSolrServer))) {
        s.shutdown();
      }
    } finally {
      server = null;
      super.stop();
    }
  }
  
  @Override
  public Status process() throws EventDeliveryException {    
    Channel ch = getChannel();
    Transaction tx = ch.getTransaction();
    try {
      tx.begin();
      int batchSize = getBatchSize();
      for (int i = 0; i < batchSize; i++) { // repeatedly take and process events from the Flume queue
        synchronized (this) { // are we asked to return control ASAP?
          if (isStopping.await(0, TimeUnit.NANOSECONDS) || isStopped.await(0, TimeUnit.NANOSECONDS)) {
            break;
          }
        }
        long startTime = System.nanoTime();
        Event event = ch.take();
        takeTimer.addAndGet(System.nanoTime() - startTime);        
        if (event == null) {
          break;
        }
        LOGGER.debug("solr event: {}", event);
        process(event);
      }
      
      if (numLoadedDocs > 0) {
        numLoadedDocs = 0;
        commitSolr();
      }
      tx.commit();
      synchronized (this) { 
        if (isStopping.await(0, TimeUnit.NANOSECONDS)) { // are we asked to return control ASAP?
          isStopped.countDown(); // signal to other thread that we're done
        }
      }
      return Status.READY;
    } catch (Exception e) {
      tx.rollback();
      throw new EventDeliveryException(e);
    } finally {
      tx.close();
    }    
  }

  /** Returns the number of events to take per flume transaction */
  protected int getBatchSize() {
    return 1000;
  }
  
  /** Extracts, transforms and loads the given Flume event into Solr */
  public void process(Event event) throws IOException, SolrServerException {
    long startTime = System.nanoTime();
    List<SolrInputDocument> docs = extract(event); // TODO: use queue to support parallel ETL across multiple CPUs?
    extractTimer.addAndGet(System.nanoTime() - startTime);
    
    startTime = System.nanoTime();
    docs = transform(docs);
    transformTimer.addAndGet(System.nanoTime() - startTime);

    startTime = System.nanoTime();
    load(docs);
    loadTimer.addAndGet(System.nanoTime() - startTime);
  }

  /** Extracts the given Flume event into zero or more Solr documents */
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
    if (docs.size() > 0) {
      UpdateResponse rsp = getSolrServer().add(docs);
      numLoadedDocs += docs.size();
    }
  }

  /**
   * Sends any outstanding documents to solr and waits for a positive or negative ack (i.e. exception) from solr.
   * Depending on the outcome the caller should then commit or rollback the current flume transaction correspondingly.
   */
  public void commitSolr() {
    SolrServer s = getSolrServer();
    if (s instanceof ConcurrentUpdateSolrServer) {
      try {
        ((ConcurrentUpdateSolrServer) s).blockUntilFinished();
      } finally {
        if (s instanceof SafeConcurrentUpdateSolrServer) {
          ((SafeConcurrentUpdateSolrServer) s).clearException();
        }
      }
    }
  }
  
  @Override
  public String toString() { // debug only
    return "takeTimer[ms]=" + (takeTimer.get() / 1000000) + ", extractTimer[ms]=" + (extractTimer.get() / 1000000) + ", transformTimer[ms]=" + (transformTimer.get() / 1000000) + ", loadTimer[ms]=" + (loadTimer.get() / 1000000);
  }
  
}
