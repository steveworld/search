/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.hadoop.morphline;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.hadoop.HdfsFileFieldNames;
import org.apache.solr.hadoop.HeartBeater;
import org.apache.solr.hadoop.PathParts;
import org.apache.solr.hadoop.SolrInputDocumentWritable;
import org.apache.solr.hadoop.SolrMapper;
import org.apache.solr.morphline.DocumentLoader;
import org.apache.solr.morphline.FaultTolerance;
import org.apache.solr.morphline.SolrLocator;
import org.apache.solr.morphline.SolrMorphlineContext;
import org.apache.solr.schema.IndexSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.Compiler;
import com.cloudera.cdk.morphline.base.Fields;
import com.cloudera.cdk.morphline.base.Notifications;
import com.google.common.base.Joiner;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

/**
 * This class takes the input files, extracts the relevant content, transforms
 * it and hands SolrInputDocuments to a set of reducers.
 * 
 * More specifically, it consumes a list of <offset, hdfsFilePath> input pairs.
 * For each such pair extracts a set of zero or more SolrInputDocuments and
 * sends them to a downstream Reducer. The key for the reducer is the unique id
 * of the SolrInputDocument specified in Solr schema.xml.
 */
public class MorphlineMapper extends SolrMapper<LongWritable, Text> {

  private SolrMorphlineContext morphlineContext;
  private Command morphline;
  private Context context;
  private IndexSchema schema;
  private HeartBeater heartBeater;
  private Map<String, String> commandLineMorphlineHeaders;
  private boolean disableFileOpen;

  public static final String MORPHLINE_FILE_PARAM = "morphlineFile";
  public static final String MORPHLINE_ID_PARAM = "morphlineId";
  

  /**
   * Headers, including MIME types, can also explicitly be passed by force from the CLI to Morphline, e.g:
   * hadoop ... -D org.apache.solr.hadoop.morphline.MorphlineMapper.header._attachment_mimetype=text/comma-separated-values
   */
  public static final String MORPHLINE_HEADER_PREFIX = MorphlineMapper.class.getName() + ".header.";
  
  /**
   * Flag to disable reading of file contents if indexing just file metadata is sufficient. 
   * This improves performance and confidentiality.
   */
  public static final String DISABLE_FILE_OPEN = MorphlineMapper.class.getName() + ".disableFileOpen";
  
  private static final Logger LOG = LoggerFactory.getLogger(MorphlineMapper.class);
  
  protected IndexSchema getSchema() {
    return schema;
  }

  protected Context getContext() {
    return context;
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    this.context = context;
    
    if (LOG.isTraceEnabled()) {
      LOG.trace("CWD is {}", new File(".").getCanonicalPath());
      TreeMap map = new TreeMap();
      for (Map.Entry<String,String> entry : context.getConfiguration()) {
        map.put(entry.getKey(), entry.getValue());
      }
      LOG.trace("Configuration:\n{}", Joiner.on("\n").join(map.entrySet()));
    }
    
    FaultTolerance faultTolerance = new FaultTolerance(
        context.getConfiguration().getBoolean(FaultTolerance.IS_PRODUCTION_MODE, false), 
        context.getConfiguration().getBoolean(FaultTolerance.IS_IGNORING_RECOVERABLE_EXCEPTIONS, false));
    
    morphlineContext = (SolrMorphlineContext) new SolrMorphlineContext.Builder()
      .setDocumentLoader(new MyDocumentLoader())
      .setFaultTolerance(faultTolerance)
      .setMetricsRegistry(new MetricsRegistry())
      .build();

    class MySolrLocator extends SolrLocator { // trick to access protected ctor
      public MySolrLocator(SolrMorphlineContext ctx) {
        super(ctx);
      }
    }

    SolrLocator locator = new MySolrLocator(morphlineContext);
    locator.setSolrHomeDir(getSolrHomeDir().toString());
    schema = locator.getIndexSchema();

    // rebuild context, now with schema
    morphlineContext = (SolrMorphlineContext) new SolrMorphlineContext.Builder()
      .setIndexSchema(schema)
      .setDocumentLoader(morphlineContext.getDocumentLoader())
      .setFaultTolerance(faultTolerance)
      .setMetricsRegistry(morphlineContext.getMetricsRegistry())
      .build();

    String morphlineFile = context.getConfiguration().get(MORPHLINE_FILE_PARAM);
    String morphlineId = context.getConfiguration().get(MORPHLINE_ID_PARAM);
    morphline = new Compiler().compile(new File(morphlineFile), morphlineId, morphlineContext);

    disableFileOpen = context.getConfiguration().getBoolean(DISABLE_FILE_OPEN, false);
    LOG.debug("disableFileOpen: {}", disableFileOpen);
        
    commandLineMorphlineHeaders = new HashMap();
    for (Map.Entry<String,String> entry : context.getConfiguration()) {     
      if (entry.getKey().startsWith(MORPHLINE_HEADER_PREFIX)) {
        commandLineMorphlineHeaders.put(entry.getKey().substring(MORPHLINE_HEADER_PREFIX.length()), entry.getValue());
      }
    }
    LOG.debug("Headers, including MIME types, passed by force from the CLI to morphline: {}", commandLineMorphlineHeaders);
    
    Notifications.notifyBeginTransaction(morphline);
    heartBeater = new HeartBeater(context);
  }

  /**
   * Extract content from the path specified in the value. Key is useless.
   */
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    heartBeater.needHeartBeat();
    try {
      LOG.info("Processing file {}", value);
      InputStream in = null;
      try {
        PathParts parts = new PathParts(value.toString(), context.getConfiguration());
        Record tikaHeaders = getHeaders(parts);
        if (tikaHeaders == null) {
          return; // ignore
        }
        for (Map.Entry<String, String> entry : commandLineMorphlineHeaders.entrySet()) {
          tikaHeaders.replaceValues(entry.getKey(), entry.getValue());
        }
        long fileLength = parts.getFileStatus().getLen();
        if (disableFileOpen) {
          in = new ByteArrayInputStream(new byte[0]);
        } else {
          in = new BufferedInputStream(parts.getFileSystem().open(parts.getUploadPath()));
        }
        tikaHeaders.put(Fields.ATTACHMENT_BODY, in);
        Notifications.notifyStartSession(morphline);
        morphline.process(tikaHeaders);
        context.getCounter(MorphlineCounters.class.getName(), MorphlineCounters.FILES_READ.toString()).increment(1);
        context.getCounter(MorphlineCounters.class.getName(), MorphlineCounters.FILE_BYTES_READ.toString()).increment(fileLength);
      } catch (Exception e) {
        LOG.error("Unable to process file " + value, e);
        context.getCounter(getClass().getName() + ".errors", e.getClass().getName()).increment(1);
        FaultTolerance faultTolerance = morphlineContext.getFaultTolerance();
        if (faultTolerance.isProductionMode() && (!faultTolerance.isRecoverableException(e) || faultTolerance.isIgnoringRecoverableExceptions())) {
          ; // ignore
        } else {
          throw new IllegalArgumentException(e);          
        }
      } finally {
        if (in != null) {
          in.close();
        }
      }
    } finally {
      heartBeater.cancelHeartBeat();
    }
  }
  
  protected Record getHeaders(PathParts parts) {
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
    headers.put(getSchema().getUniqueKeyField().getName(), parts.getId()); // use HDFS file path as docId if no docId is specified
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

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    heartBeater.close();
    addMetricsToMRCounters(morphlineContext.getMetricsRegistry(), context);
    Notifications.notifyCommitTransaction(morphline);
    Notifications.notifyShutdown(morphline);
    super.cleanup(context);
  }

  private void addMetricsToMRCounters(MetricsRegistry metricsRegistry, Context context) {
    for (Map.Entry<MetricName, Metric> entry : metricsRegistry.allMetrics().entrySet()) {
      // only add counter metrics
      if (entry.getValue() instanceof Counter) {
        Counter c = (Counter)entry.getValue();
        MetricName metricName = entry.getKey();
        context.getCounter(metricName.getGroup() + "." + metricName.getType(),
          metricName.getName()).increment(c.count());
      }
    }
  }
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private final class MyDocumentLoader implements DocumentLoader {

    @Override
    public void beginTransaction() {
    }

    @Override
    public void load(SolrInputDocument doc) throws IOException, SolrServerException {
      String uniqueKeyFieldName = getSchema().getUniqueKeyField().getName();
      String id = doc.getFieldValue(uniqueKeyFieldName).toString();
      try {
        context.write(new Text(id), new SolrInputDocumentWritable(doc));
      } catch (InterruptedException e) {
        throw new IOException("Interrupted while writing " + doc, e);
      }

      if (LOG.isDebugEnabled()) {
        long numParserOutputBytes = 0;
        for (SolrInputField field : doc.values()) {
          numParserOutputBytes += sizeOf(field.getValue());
        }
        context.getCounter(MorphlineCounters.class.getName(), MorphlineCounters.PARSER_OUTPUT_BYTES.toString()).increment(numParserOutputBytes);
      }
      context.getCounter(MorphlineCounters.class.getName(), MorphlineCounters.DOCS_READ.toString()).increment(1);
    }

    // just an approximation
    private long sizeOf(Object value) {
      if (value instanceof CharSequence) {
        return ((CharSequence) value).length();
      } else if (value instanceof Integer) {
        return 4;
      } else if (value instanceof Long) {
        return 8;
      } else if (value instanceof Collection) {
        long size = 0;
        for (Object val : (Collection) value) {
          size += sizeOf(val);
        }
        return size;      
      } else {
        return String.valueOf(value).length();
      }
    }

    @Override
    public void commitTransaction() {
    }

    @Override
    public UpdateResponse rollbackTransaction() throws SolrServerException, IOException {
      return new UpdateResponse();
    }

    @Override
    public void shutdown() {
    }

    @Override
    public SolrPingResponse ping() throws SolrServerException, IOException {
      return new SolrPingResponse();
    }

  }

}
