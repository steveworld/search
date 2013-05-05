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

import java.io.File;
import java.io.IOException;
import java.util.Map.Entry;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.morphline.FaultTolerance;
import org.apache.solr.morphline.SolrMorphlineContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.Configs;
import com.cloudera.cdk.morphline.api.MorphlineCompilationException;
import com.cloudera.cdk.morphline.api.MorphlineRuntimeException;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.Fields;
import com.cloudera.cdk.morphline.base.Notifications;
import com.cloudera.cdk.morphline.cmd.DropRecordBuilder;
import com.cloudera.cdk.morphline.cmd.PipeBuilder;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.yammer.metrics.core.MetricsRegistry;

/**
 * A {@link SolrIndexer} that processes it's events using a morphline {@link Command} chain in order
 * to load them into Solr.
 */
public class MorphlineSolrIndexer implements SolrIndexer {

  private SolrMorphlineContext morphlineContext;
  private Command morphline;
  
  public static final String MORPHLINE_FILE_PARAM = "morphlineFile";
  public static final String MORPHLINE_ID_PARAM = "morphlineId";
  
  private static final Logger LOG = LoggerFactory.getLogger(MorphlineSolrIndexer.class);

  // For test injection
  protected void setMorphlineContext(SolrMorphlineContext morphlineContext) {
    this.morphlineContext = morphlineContext;
  }

  @Override
  public void configure(Context context) {
    // TODO: also support fetching morphlineFile from zk
    // e.g. via specifying an optional solrLocator here, 
    // SolrLocator later downloads schema.xml and solrconfig.xml if has zkhost with collectionname 
    String morphlineFile = context.getString(MORPHLINE_FILE_PARAM);
    if (morphlineFile == null || morphlineFile.trim().length() == 0) {
      throw new MorphlineCompilationException("Missing morphlineFile parameter", null);
    }
    Config config;
    try {
      config = Configs.parse(new File(morphlineFile));
    } catch (IOException e) {
      throw new MorphlineCompilationException("Cannot compile morphline: " + morphlineFile, null);
    }
    
    String morphlineName = context.getString(MORPHLINE_ID_PARAM);
    if (morphlineName != null) {
      morphlineName = morphlineName.trim();
    }
    if (morphlineName != null && morphlineName.length() == 0) {
      morphlineName = null;
    }
    if (morphlineName == null) {
      config = config.getConfigList("morphlines").get(0);
      Preconditions.checkNotNull(config);
    } else {
      for (Config candidate : config.getConfigList("morphlines")) {
        if (morphlineName.equals(Configs.getString(candidate, "id", null))) {
          config = candidate;
          break;
        }
      }
      Preconditions.checkNotNull(config);
    }
    
    if (morphlineContext == null) {
      FaultTolerance faultTolerance = new FaultTolerance(
          context.getBoolean(FaultTolerance.IS_PRODUCTION_MODE, false), 
          context.getBoolean(FaultTolerance.IS_IGNORING_RECOVERABLE_EXCEPTIONS, false));
      
      morphlineContext = (SolrMorphlineContext) new SolrMorphlineContext.Builder()
        .setFaultTolerance(faultTolerance)
        .setMetricsRegistry(new MetricsRegistry())
        .build();
    }
    Command dropRecord = new DropRecordBuilder().build(null, null,  null, morphlineContext);
    morphline = new PipeBuilder().build(config, null, dropRecord, morphlineContext);
  }

  @Override
  public void process(Event event) throws IOException, SolrServerException {
    Record record = new Record();
    for (Entry<String, String> entry : event.getHeaders().entrySet()) {
      record.put(entry.getKey(), entry.getValue());
    }
    byte[] bytes = event.getBody();
    if (bytes != null) {
      record.put(Fields.ATTACHMENT_BODY, bytes);
    }    
    try {
      Notifications.notifyStartSession(morphline);
      morphline.process(record);
    } catch (RuntimeException t) {
      handleException(t, record);
    }
  }

  private void handleException(Throwable t, Record event) {
    if (t instanceof Error) {
      throw (Error) t; // never ignore errors
    }
    FaultTolerance faultTolerance = morphlineContext.getFaultTolerance();
    if (faultTolerance.isProductionMode()) {
      if (!faultTolerance.isRecoverableException(t)) {
        LOG.warn("Ignoring unrecoverable exception in production mode for event: " + event, t);
        return;
      } else if (faultTolerance.isIgnoringRecoverableExceptions()) {
        LOG.warn("Ignoring recoverable exception in production mode for event: " + event, t);
        return;
      }
    }
    throw new MorphlineRuntimeException(t);
  }
  
  @Override
  public void beginTransaction() throws IOException, SolrServerException {
    Notifications.notifyBeginTransaction(morphline);      
  }

  @Override
  public void commitTransaction() throws IOException, SolrServerException {
    Notifications.notifyCommitTransaction(morphline);      
  }

  @Override
  public void rollbackTransaction() throws IOException, SolrServerException {
    Notifications.notifyRollbackTransaction(morphline);            
  }

  @Override
  public void stop() {
    Notifications.notifyShutdown(morphline);
  }

}
