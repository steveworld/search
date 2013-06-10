/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.sink.solr.morphline;

import java.io.File;
import java.util.Map.Entry;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.solr.morphline.FaultTolerance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.MorphlineCompilationException;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.Compiler;
import com.cloudera.cdk.morphline.base.Fields;
import com.cloudera.cdk.morphline.base.Notifications;
import com.codahale.metrics.MetricRegistry;

/**
 * A {@link SolrIndexer} that processes it's events using a morphline {@link Command} chain in order
 * to load them into Solr.
 */
public class MorphlineSolrIndexer implements SolrIndexer {

  private MorphlineContext morphlineContext;
  private Command morphline;
  private Command finalChild;
  private String morphlineFileAndId;
  
  public static final String MORPHLINE_FILE_PARAM = "morphlineFile";
  public static final String MORPHLINE_ID_PARAM = "morphlineId";

  private static final Logger LOG = LoggerFactory.getLogger(MorphlineSolrIndexer.class);
  
  // For test injection
  void setMorphlineContext(MorphlineContext morphlineContext) {
    this.morphlineContext = morphlineContext;
  }

  // for interceptor
  void setFinalChild(Command finalChild) {
    this.finalChild = finalChild;
  }

  @Override
  public void configure(Context context) {
    if (morphlineContext == null) {
      FaultTolerance faultTolerance = new FaultTolerance(
          context.getBoolean(FaultTolerance.IS_PRODUCTION_MODE, false), 
          context.getBoolean(FaultTolerance.IS_IGNORING_RECOVERABLE_EXCEPTIONS, false));
      
      morphlineContext = new MorphlineContext.Builder()
        .setExceptionHandler(faultTolerance)
        .setMetricRegistry(new MetricRegistry())
        .build();
    }
    
    String morphlineFile = context.getString(MORPHLINE_FILE_PARAM);
    String morphlineId = context.getString(MORPHLINE_ID_PARAM);
    if (morphlineFile == null || morphlineFile.trim().length() == 0) {
      throw new MorphlineCompilationException("Missing parameter: " + MORPHLINE_FILE_PARAM, null);
    }
    morphline = new Compiler().compile(new File(morphlineFile), morphlineId, morphlineContext, finalChild);
    morphlineFileAndId = morphlineFile + "@" + morphlineId;
  }

  @Override
  public void process(Event event) {
    Record record = new Record();
    for (Entry<String, String> entry : event.getHeaders().entrySet()) {
      record.put(entry.getKey(), entry.getValue());
    }
    byte[] bytes = event.getBody();
    if (bytes != null && bytes.length > 0) {
      record.put(Fields.ATTACHMENT_BODY, bytes);
    }    
    try {
      Notifications.notifyStartSession(morphline);
      if (!morphline.process(record)) {
        LOG.warn("Morphline {} failed to process record: {}", morphlineFileAndId, record);
      }
    } catch (RuntimeException t) {
      morphlineContext.getExceptionHandler().handleException(t, record);
    }
  }

  @Override
  public void beginTransaction() {
    Notifications.notifyBeginTransaction(morphline);      
  }

  @Override
  public void commitTransaction() {
    Notifications.notifyCommitTransaction(morphline);      
  }

  @Override
  public void rollbackTransaction() {
    Notifications.notifyRollbackTransaction(morphline);            
  }

  @Override
  public void stop() {
    Notifications.notifyShutdown(morphline);
  }

}
