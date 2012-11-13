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

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.flume.sink.solr.indexer.Configuration;
import org.apache.flume.sink.solr.indexer.SimpleIndexer;
import org.apache.flume.sink.solr.indexer.StreamEvent;
import org.apache.flume.sink.solr.indexer.TikaIndexer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
//import org.apache.flume.annotations.InterfaceStability;

/**
 * EXPERIMENTAL API; Flume sink that extracts search documents from Apache Flume events,
 * transforms them and loads them into Apache Solr.
 */
// @InterfaceStability.Evolving
public class SolrSink extends AbstractSink implements Configurable {

  private int maxBatchSize = 1000;
  private long maxBatchDurationMillis = 10 * 1000;
  private SimpleIndexer indexer;
  private Context context;
  private SinkCounter sinkCounter; // TODO: replace with metrics.codahale.com

  public static final String BATCH_SIZE = "batchSize";
  public static final String BATCH_DURATION_MILLIS = "batchDurationMillis";
  public static final String INDEXER_CLASS = "indexerClass";
  
  private static final Logger LOGGER = LoggerFactory.getLogger(SolrSink.class);

  public SolrSink() {
    this(null);
  }

  /** For testing only */
  protected SolrSink(SimpleIndexer indexer) {
    this.indexer = indexer;
  }

  public final SimpleIndexer getIndexer() {
    return indexer;
  }
  
  @Override
  public void configure(Context context) {
    this.context = context;
    maxBatchSize = context.getInteger(BATCH_SIZE, maxBatchSize);
    maxBatchDurationMillis = context.getLong(BATCH_DURATION_MILLIS, maxBatchDurationMillis);
    
    if (indexer == null) {
      String clazz = context.getString(INDEXER_CLASS, TikaIndexer.class.getName());
      try {
        indexer = (SimpleIndexer) Class.forName(clazz).newInstance();
      } catch (Exception e) {
        throw new ConfigurationException(e);
      }
    }    
    indexer.setName(getName());
//    Config config = ConfigFactory.load();    
    Config config = ConfigFactory.parseMap(context.getParameters());
    indexer.configure(new Configuration(config));
    
    if (sinkCounter == null) {
      sinkCounter = new SinkCounter(getName());
    }
  }

  /** Returns the Flume configuration settings */
  protected Context getContext() {
    return context;
  }

  /**
   * Returns the maximum number of events to take per flume transaction;
   * override to customize
   */
  protected int getMaxBatchSize() {
    return maxBatchSize;
  }

  /** Returns the maximum duration per flume transaction; override to customize */
  protected long getMaxBatchDurationMillis() {
    return maxBatchDurationMillis;
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
    sinkCounter.start();
    indexer.start();
    super.start();
    LOGGER.info("Solr sink {} started.", getName());
  }

  @Override
  public synchronized void stop() {
    LOGGER.info("Solr sink {} stopping...", getName());
    try {
      indexer.stop();
      sinkCounter.stop();
      LOGGER.info("Solr sink {} stopped. Metrics: {}, {}", getName(), sinkCounter);
    } finally {
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
      indexer.beginTransaction();
      isSolrTransactionCommitted = false;

      // repeatedly take and process events from the Flume queue
      for (int i = 0; i < batchSize; i++) {
        Event event = myChannel.take();
        if (event == null) {
          break;
        }
        numEventsTaken++;
        LOGGER.debug("solr event: {}", event);
        InputStream in = new ByteArrayInputStream(event.getBody());
        indexer.process(new StreamEvent(in, event.getHeaders()));
        if (System.currentTimeMillis() >= batchEndTime) {
          break;
        }
      }

      // update metrics
      if (numEventsTaken == 0) {
        sinkCounter.incrementBatchEmptyCount();
      }
      if (numEventsTaken < batchSize) {
        sinkCounter.incrementBatchUnderflowCount();
      } else {
        sinkCounter.incrementBatchCompleteCount();
      }
      sinkCounter.addToEventDrainAttemptCount(numEventsTaken);
      sinkCounter.addToEventDrainSuccessCount(numEventsTaken);

      indexer.commitTransaction();
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
            indexer.rollback();
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

  @Override
  public String toString() {
    int i = getClass().getName().lastIndexOf('.') + 1;
    String shortClassName = getClass().getName().substring(i);
    return shortClassName + " " + getName();
  }

}
