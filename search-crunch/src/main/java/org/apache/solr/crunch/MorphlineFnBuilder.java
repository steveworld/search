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
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.hadoop.fs.FileStatus;
import org.apache.solr.security.util.job.JobSecurityUtil;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.api.TypedSettings;
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
 * Factory for a DoFn that transforms the input with a configurable Morphline.
 */
public final class MorphlineFnBuilder<S,T> {

  private String morphlineFileContents;
  private String morphlineId;
  private Map<String, String> morphlineVariables = new HashMap<String, String>();
  private Map<String, Object> settings = new HashMap<String, Object>();
  private boolean isSplittable = false;
  
  public MorphlineFnBuilder() {}
  
  public MorphlineFnBuilder<S,T> morphlineFileContents(String morphlineFileContents) {
    if (morphlineFileContents == null || morphlineFileContents.trim().length() == 0) {
      throw new IllegalArgumentException("Missing morphlineFileContents");
    }
    this.morphlineFileContents = morphlineFileContents;
    return this;
  }
  
  public MorphlineFnBuilder<S,T> morphlineId(String morphlineId) {
    this.morphlineId = morphlineId;
    return this;
  }
  
  public MorphlineFnBuilder<S,T> morphlineVariables(Map<String, String> morphlineVariables) {
    Preconditions.checkNotNull(morphlineVariables);
    this.morphlineVariables = morphlineVariables;
    return this;
  }
  
  public MorphlineFnBuilder<S,T> morphlineSettings(Map<String,Object> settings) {
    Preconditions.checkNotNull(settings);
    this.settings = settings;
    return this;
  }
  
  public MorphlineFnBuilder<S,T> isSplittable(boolean isSplittable) {
    this.isSplittable = isSplittable;
    return this;
  }    
  
  public DoFn<S,T> build() {
    return new MorphlineFn<S,T>(
      morphlineFileContents, 
      morphlineId, 
      morphlineVariables, 
      settings, 
      isSplittable);    
  }

  
  ///////////////////////////////////////////////////////////////////////////////
  //Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class MorphlineFn<S,T> extends DoFn<S,T> {
  
    private String morphlineFileContents;
    private String morphlineId;
    private Map<String, String> morphlineVariables;
    private Map<String, Object> morphlineSettings;
    private boolean isSplittable;
    
    private transient MorphlineContext morphlineContext;
    private transient Command morphline;
    private transient Collector collector;
  
    private transient Timer mappingTimer;
    private transient Meter numRecords;
    private transient Meter numFailedRecords;
    private transient Meter numExceptionRecords;
  
    static final String METRICS_GROUP_NAME = "morphline";
    static final String METRICS_LIVE_COUNTER_NAME = MetricRegistry.name(Metrics.MORPHLINE_APP, Metrics.NUM_RECORDS, "live");
    
    private static final Logger LOG = LoggerFactory.getLogger(MorphlineFnBuilder.class);
    
    static {
      setupMorphlineClasspath();
    }
  
    private MorphlineFn(
        String morphlineFileContents, 
        String morphlineId, 
        Map<String, String> morphlineVariables, 
        Map<String, Object> morphlineSettings, 
        boolean isSplittable) {
      
      if (morphlineFileContents == null || morphlineFileContents.trim().length() == 0) {
        throw new IllegalArgumentException("Missing morphlineFileContents");
      }
      this.morphlineFileContents = morphlineFileContents;
      this.morphlineId = morphlineId;
      Preconditions.checkNotNull(morphlineVariables);
      this.morphlineVariables = morphlineVariables;
      Preconditions.checkNotNull(morphlineSettings);
      this.morphlineSettings = morphlineSettings;
      this.isSplittable = isSplittable;
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
  
      Map<String, Object> mySettings = new HashMap(morphlineSettings);
      mySettings.put(TypedSettings.TASK_CONTEXT_SETTING_NAME, getContext());
  
      loadSecureCredentials();
  
      morphlineContext = new MorphlineContext.Builder()
          .setExceptionHandler(faultTolerance)
          .setMetricRegistry(SharedMetricRegistries.getOrCreate(morphlineFileAndId))
          .setSettings(mySettings)
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
      // This live counter can be monitored while the task is running, not just after task termination:
      increment(METRICS_GROUP_NAME, METRICS_LIVE_COUNTER_NAME); 
      
      numRecords.mark();
      Timer.Context timerContext = mappingTimer.time();
      getContext().progress();
      InputStream in = null;
      try {
        collector.setEmitter(emitter);
        Record record;
        if (isSplittable) {
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
  
    private void loadSecureCredentials() {
      // load secure credentials if present in configuration
      String serviceName = getConfiguration().get(CrunchIndexerTool.SECURE_CONF_SERVICE_NAME);
      try {
        if (serviceName != null) {
          if (getConfiguration().get(JobSecurityUtil.CREDENTIALS_FILE_LOCATION) == null) {
            JobSecurityUtil.loadCredentialsForClients(getContext(), serviceName);
          }
          else {
            JobSecurityUtil.loadCredentialsForClients(getConfiguration(), serviceName);
          }
        }
      } catch (IOException e) {
        throw new CrunchRuntimeException(e);
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
      increment(METRICS_GROUP_NAME, metricName, value.getCount() / scale);
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
    
    /*
     * Ensure scripting support for Java via morphline "java" command works even when running inside
     * custom class loaders. To do so, collect all classpath URLs from the class loaders chain that
     * org.apache.hadoop.util.RunJar (hadoop jar xyz-job.jar) and
     * org.apache.hadoop.util.GenericOptionsParser (--libjars) or similar have installed, then tell
     * FastJavaScriptEngine.parse() where to find classes that JavaBuilder scripts might depend on.
     * This ensures that scripts that reference external java classes compile without exceptions like
     * this:
     * 
     * ... caused by compilation failed: mfm:///MyJavaClass1.java:2: package
     * org.kitesdk.morphline.api does not exist
     */
    private static void setupMorphlineClasspath()  {
      LOG.trace("dryRun: java.class.path: {}", System.getProperty("java.class.path"));
      String fullClassPath = "";
      ClassLoader loader = Thread.currentThread().getContextClassLoader(); // see org.apache.hadoop.util.RunJar
      while (loader != null) { // walk class loaders, collect all classpath URLs
        if (loader instanceof URLClassLoader) { 
          URL[] classPathPartURLs = ((URLClassLoader) loader).getURLs(); // see org.apache.hadoop.util.RunJar
          LOG.trace("dryRun: classPathPartURLs: {}", Arrays.asList(classPathPartURLs));
          StringBuilder classPathParts = new StringBuilder();
          for (URL url : classPathPartURLs) {
            File file;
            try {
              file = new File(url.toURI());
            } catch (URISyntaxException e) {
              throw new RuntimeException(e);
            }
            if (classPathPartURLs.length > 0) {
              classPathParts.append(File.pathSeparator);
            }
            classPathParts.append(file.getPath());
          }
          LOG.trace("dryRun: classPathParts: {}", classPathParts);
          String separator = File.pathSeparator;
          if (fullClassPath.length() == 0 || classPathParts.length() == 0) {
            separator = "";
          }
          fullClassPath = classPathParts + separator + fullClassPath;
        }
        loader = loader.getParent();
      }
      
      // tell FastJavaScriptEngine.parse() where to find the classes that the script might depend on
      if (fullClassPath.length() > 0) {
        assert System.getProperty("java.class.path") != null;
        fullClassPath = System.getProperty("java.class.path") + File.pathSeparator + fullClassPath;
        LOG.trace("dryRun: fullClassPath: {}", fullClassPath);
        System.setProperty("java.class.path", fullClassPath); // see FastJavaScriptEngine.parse()
      }    
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
}
