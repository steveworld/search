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
package org.apache.solr.crunch;


import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.hadoop.fs.FileStatus;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.Compiler;
import org.kitesdk.morphline.base.FaultTolerance;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.base.Metrics;
import org.kitesdk.morphline.base.Notifications;
import org.kitesdk.morphline.shaded.com.google.common.io.Closeables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Counting;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;


/**
 * Transforms the input with a configurable Morphline.
 */
public class MorphlineFn<S,T> extends DoFn<S,T> {

  private String morphlineFileContents;
  private String morphlineId;
  private Map<String, String> morphlineVariables;
  private boolean isSplitable;
  
  private transient MorphlineContext morphlineContext;
  private transient Command morphline;
  private transient Collector collector;

  private transient Timer mappingTimer;
  private transient Meter numRecords;
  private transient Meter numFailedRecords;
  private transient Meter numExceptionRecords;

  private static final Logger LOG = LoggerFactory.getLogger(MorphlineFn.class);

  public MorphlineFn(String morphlineFileContents, String morphlineId, Map<String, String> morphlineVariables, boolean isSplitable) {
    if (morphlineFileContents == null || morphlineFileContents.trim().length() == 0) {
      throw new IllegalArgumentException("Missing morphlineFileContents");
    }
    this.morphlineFileContents = morphlineFileContents;
    this.morphlineId = morphlineId;
    Preconditions.checkNotNull(morphlineVariables);
    this.morphlineVariables = morphlineVariables;
    this.isSplitable = isSplitable;
  }

  @Override
  public void initialize() {
    Utils.getLogConfigFile(getConfiguration());
    if (LOG.isTraceEnabled()) {
      TreeMap map = new TreeMap();
      for (Map.Entry<String,String> entry : getConfiguration()) {
        map.put(entry.getKey(), entry.getValue());
      }
      LOG.trace("Configuration:\n{}", Joiner.on("\n").join(map.entrySet()));
    }

    String morphlineFileAndId = UUID.randomUUID() + "@" + morphlineId;

    FaultTolerance faultTolerance = new FaultTolerance(
        getConfiguration().getBoolean(FaultTolerance.IS_PRODUCTION_MODE, false),
        getConfiguration().getBoolean(FaultTolerance.IS_IGNORING_RECOVERABLE_EXCEPTIONS, false),
        getConfiguration().get(FaultTolerance.RECOVERABLE_EXCEPTION_CLASSES));

    morphlineContext = new MorphlineContext.Builder()
        .setExceptionHandler(faultTolerance)
        .setMetricRegistry(SharedMetricRegistries.getOrCreate(morphlineFileAndId))
        .build();

    Config override = ConfigFactory.parseMap(morphlineVariables);

    File morphlineTmpFile;
    try {
      morphlineTmpFile = File.createTempFile(getClass().getName(), ".tmp");
    } catch (IOException e) {
      throw new CrunchRuntimeException(e);
    }

    try {
      Files.write(morphlineFileContents, morphlineTmpFile, Charsets.UTF_8);
      collector = new Collector();
      morphline = new Compiler().compile(morphlineTmpFile, morphlineId, morphlineContext, collector, override);
    } catch (Exception e) {
      throw new CrunchRuntimeException(e);
    } finally {
      morphlineTmpFile.delete();
    }

    this.mappingTimer = morphlineContext.getMetricRegistry().timer(
        MetricRegistry.name(Metrics.MORPHLINE_APP, Metrics.ELAPSED_TIME));
    this.numRecords = morphlineContext.getMetricRegistry().meter(
        MetricRegistry.name(Metrics.MORPHLINE_APP, Metrics.NUM_RECORDS));
    this.numFailedRecords = morphlineContext.getMetricRegistry().meter(
        MetricRegistry.name(Metrics.MORPHLINE_APP, Metrics.NUM_FAILED_RECORDS));
    this.numExceptionRecords = morphlineContext.getMetricRegistry().meter(
        MetricRegistry.name(Metrics.MORPHLINE_APP, Metrics.NUM_EXCEPTION_RECORDS));

    Notifications.notifyBeginTransaction(morphline);
  }

  @Override
  public void process(S item, Emitter<T> emitter) {
    numRecords.mark();
    Timer.Context timerContext = mappingTimer.time();
    getContext().progress();
    InputStream in = null;
    try {
      collector.setEmitter(emitter);
      Record record;
      if (isSplitable) {
        record = new Record();
        record.put(Fields.ATTACHMENT_BODY, item);
      } else {
        PathParts parts = new PathParts(item.toString(), getConfiguration());
        record = getRecord(parts);
        if (record == null) {
          return; // ignore
        }
        in = new BufferedInputStream(parts.getFileSystem().open(parts.getUploadPath()));
        record.put(Fields.ATTACHMENT_BODY, in);
      }
      try {
        Notifications.notifyStartSession(morphline);
        if (!morphline.process(record)) {
          numFailedRecords.mark();
          LOG.warn("Morphline failed to process record: {}", record);
        }
      } catch (RuntimeException t) {
        numExceptionRecords.mark();
        morphlineContext.getExceptionHandler().handleException(t, record);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (in != null) {
        Closeables.closeQuietly(in);
      }
      timerContext.stop();
    }
  }

  @Override
  public void cleanup(Emitter<T> emitter) {
    try {
      collector.setEmitter(emitter);
      Notifications.notifyCommitTransaction(morphline);
      Notifications.notifyShutdown(morphline);
    } finally {
      addMetricsToMRCounters(morphlineContext.getMetricRegistry());
    }
  }

  private void addMetricsToMRCounters(MetricRegistry metricRegistry) {
    for (Map.Entry<String, Counter> entry : metricRegistry.getCounters().entrySet()) {
      addCounting(entry.getKey(),  entry.getValue(), 1);
    }
    for (Map.Entry<String, Histogram> entry : metricRegistry.getHistograms().entrySet()) {
      addCounting(entry.getKey(),  entry.getValue(), 1);
    }
    for (Map.Entry<String, Meter> entry : metricRegistry.getMeters().entrySet()) {
      addCounting(entry.getKey(), entry.getValue(), 1);
    }
    for (Map.Entry<String, Timer> entry : metricRegistry.getTimers().entrySet()) {
      long nanosPerMilliSec = 1000 * 1000;
      addCounting(entry.getKey(), entry.getValue(), nanosPerMilliSec);
    }
  }

  private void addCounting(String metricName, Counting value, long scale) {
    increment("morphline", metricName, value.getCount() / scale);
  }

  private Record getRecord(PathParts parts) {
    FileStatus stats;
    try {
      stats = parts.getFileStatus();
    } catch (IOException e) {
      stats = null;
    }
    if (stats == null) {
      LOG.warn("Ignoring file that somehow has become unavailable since the job was submitted: {}",
          parts.getUploadURL());
      return null;
    }
    
    Record headers = new Record();
    //headers.put(getSchema().getUniqueKeyField().getName(), parts.getId()); // use HDFS file path as docId if no docId is specified
    headers.put(Fields.BASE_ID, parts.getId()); // with sanitizeUniqueKey command, use HDFS file path as docId if no docId is specified
    headers.put(Fields.ATTACHMENT_NAME, parts.getName()); // Tika can use the file name in guessing the right MIME type
    
    // enable indexing and storing of file meta data in Solr
    headers.put(HdfsFileFieldNames.FILE_UPLOAD_URL, parts.getUploadURL());
    headers.put(HdfsFileFieldNames.FILE_DOWNLOAD_URL, parts.getDownloadURL());
    headers.put(HdfsFileFieldNames.FILE_SCHEME, parts.getScheme()); 
    headers.put(HdfsFileFieldNames.FILE_HOST, parts.getHost()); 
    headers.put(HdfsFileFieldNames.FILE_PORT, String.valueOf(parts.getPort())); 
    headers.put(HdfsFileFieldNames.FILE_PATH, parts.getURIPath()); 
    headers.put(HdfsFileFieldNames.FILE_NAME, parts.getName());     
    headers.put(HdfsFileFieldNames.FILE_LAST_MODIFIED, String.valueOf(stats.getModificationTime())); // FIXME also add in SpoolDirectorySource
    headers.put(HdfsFileFieldNames.FILE_LENGTH, String.valueOf(stats.getLen())); // FIXME also add in SpoolDirectorySource
    headers.put(HdfsFileFieldNames.FILE_OWNER, stats.getOwner());
    headers.put(HdfsFileFieldNames.FILE_GROUP, stats.getGroup());
    headers.put(HdfsFileFieldNames.FILE_PERMISSIONS_USER, stats.getPermission().getUserAction().SYMBOL);
    headers.put(HdfsFileFieldNames.FILE_PERMISSIONS_GROUP, stats.getPermission().getGroupAction().SYMBOL);
    headers.put(HdfsFileFieldNames.FILE_PERMISSIONS_OTHER, stats.getPermission().getOtherAction().SYMBOL);
    headers.put(HdfsFileFieldNames.FILE_PERMISSIONS_STICKYBIT, String.valueOf(stats.getPermission().getStickyBit()));
    // TODO: consider to add stats.getAccessTime(), stats.getReplication(), stats.isSymlink(), stats.getBlockSize()
    
    return headers;
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class Collector implements Command {

    private Emitter emitter;

    public Collector() {}

    public void setEmitter(Emitter emitter) {
      this.emitter = emitter;
    }

    @Override
    public Command getParent() {
      return null;
    }

    @Override
    public void notify(Record notification) {
    }

    @Override
    public boolean process(Record record) {
      Preconditions.checkNotNull(record);
      for (Object attachment : record.get(Fields.ATTACHMENT_BODY)) {
        emitter.emit(attachment);
      }
      return true;
    }

  }

}
