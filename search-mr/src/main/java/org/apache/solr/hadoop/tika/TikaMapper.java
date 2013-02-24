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
package org.apache.solr.hadoop.tika;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.DateUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.hadoop.HeartBeater;
import org.apache.solr.hadoop.PathParts;
import org.apache.solr.hadoop.SolrInputDocumentWritable;
import org.apache.solr.hadoop.SolrMapper;
import org.apache.solr.hadoop.dedup.RetainMostRecentUpdateConflictResolver;
import org.apache.solr.handler.extraction.ExtractingRequestHandler;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.tika.ConfigurationException;
import org.apache.solr.tika.DocumentLoader;
import org.apache.solr.tika.SolrCollection;
import org.apache.solr.tika.SolrIndexer;
import org.apache.solr.tika.StreamEvent;
import org.apache.solr.tika.TikaIndexer;
import org.apache.tika.metadata.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.google.common.base.Joiner;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TikaMapper extends SolrMapper<LongWritable, Text> {

  private SolrIndexer indexer;
  private Context context;
  private IndexSchema schema;
  private HeartBeater heartBeater;

  public static final String FILE_DOWNLOAD_URL_FIELD_NAME = "file_download_url";
  public static final String FILE_SCHEME_FIELD_NAME = "file_scheme";
  public static final String FILE_HOST_FIELD_NAME = "file_host";
  public static final String FILE_PORT_FIELD_NAME = "file_port";
  public static final String FILE_PATH_FIELD_NAME = "file_path";
  public static final String FILE_NAME_FIELD_NAME = "file_name";
  public static final String FILE_LENGTH_FIELD_NAME = "file_length";
  public static final String FILE_LAST_MODIFIED_FIELD_NAME = RetainMostRecentUpdateConflictResolver.ORDER_BY_FIELD_NAME_DEFAULT;

  private static final Logger LOG = LoggerFactory.getLogger(TikaMapper.class);
  
  protected IndexSchema getSchema() {
    return schema;
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    if (LOG.isTraceEnabled()) {
      LOG.trace("CWD is {}", new File(".").getCanonicalPath());
      TreeMap map = new TreeMap();
      for (Map.Entry<String,String> entry : context.getConfiguration()) {
        map.put(entry.getKey(), entry.getValue());
      }
      LOG.trace("Configuration:\n{}", Joiner.on("\n").join(map.entrySet()));
    }
    this.context = context;
    Map<String, Object> params = new HashMap<String,Object>();
    for (Map.Entry<String,String> entry : context.getConfiguration()) {
      if (entry.getValue() != null && (entry.getKey().contains("tika") || entry.getKey().contains("Tika"))) {
        params.put(entry.getKey(), entry.getValue());
      }
    }
    String tikaConfigLocation = context.getConfiguration().get(TikaIndexer.TIKA_CONFIG_LOCATION);
    if (tikaConfigLocation != null) {
      params.put(TikaIndexer.TIKA_CONFIG_LOCATION, tikaConfigLocation);      
//    } else {
//      throw new IllegalStateException("Missing tika.config parameter"); // for debugging      
    }
    Config config = ConfigFactory.parseMap(params);
    indexer = createSolrIndexer(context);
    indexer.configure(config);
    indexer.start();
    for (SolrCollection collection : indexer.getSolrCollections().values()) {
      schema = collection.getSchema();
    }
    if (schema == null) {
      throw new IllegalStateException("Schema must not be null");
    }
    indexer.beginTransaction();
    heartBeater = new HeartBeater(context);
  }

  protected SolrIndexer createSolrIndexer(Context context) {
    return new MyTikaIndexer();
  }

  /**
   * Extract content from the path specified in the value. Key is useless.
   */
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    heartBeater.needHeartBeat();
    try {
      LOG.info("Processing file {}", value);
      PathParts parts = new PathParts(value.toString(), context.getConfiguration());
      Map<String,String> headers = getHeaders(parts);
      if (headers == null) {
        return; // ignore
      }
      long fileLength = parts.getFileStatus().getLen();
      FSDataInputStream in = parts.getFileSystem().open(parts.getDownloadPath());
      try {
        indexer.process(new StreamEvent(in, headers));
        context.getCounter(TikaCounters.class.getName(), TikaCounters.FILES_READ.toString()).increment(1);
        context.getCounter(TikaCounters.class.getName(), TikaCounters.FILE_BYTES_READ.toString()).increment(fileLength);
      } catch (Exception e) {
        context.getCounter(getClass().getName() + ".errors", e.getClass().getName()).increment(1);
        LOG.error("Unable to process file " + value, e);
      } finally {
        in.close();
      }
    } finally {
      heartBeater.cancelHeartBeat();
    }
  }
  
  protected Map<String, String> getHeaders(PathParts parts) {
    String downloadURL = parts.getDownloadURL();
    FileStatus stats;
    try {
      stats = parts.getFileStatus();
    } catch (IOException e) {
      stats = null;
    }
    if (stats == null) {
      LOG.warn("Ignoring file that somehow has become unavailable since the job was submitted: {}", downloadURL);
      return null;
    }
    
    Map<String,String> headers = new HashMap<String, String>();
    headers.put(getSchema().getUniqueKeyField().getName(), parts.getId()); // use HDFS file path as docId if no docId is specified
    headers.put(Metadata.RESOURCE_NAME_KEY, parts.getName()); // Tika can use the file name in guessing the right MIME type
    
    // enable indexing and storing of file meta data in Solr
    headers.put(FILE_DOWNLOAD_URL_FIELD_NAME, parts.getDownloadURL());
    headers.put(FILE_SCHEME_FIELD_NAME, parts.getScheme()); 
    headers.put(FILE_HOST_FIELD_NAME, parts.getHost()); 
    headers.put(FILE_PORT_FIELD_NAME, String.valueOf(parts.getPort())); 
    headers.put(FILE_PATH_FIELD_NAME, parts.getURIPath()); 
    headers.put(FILE_NAME_FIELD_NAME, parts.getName());     
    headers.put(FILE_LAST_MODIFIED_FIELD_NAME, String.valueOf(stats.getModificationTime())); // FIXME also in SpoolDirSource
    headers.put(FILE_LENGTH_FIELD_NAME, String.valueOf(stats.getLen())); // FIXME also in SpoolDirSource
    
    // TODO: also add owner, group, perms, file extension?
    return headers;
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    heartBeater.close();
    super.cleanup(context);
    indexer.commitTransaction();
    indexer.stop();
  }

  private class MyTikaIndexer extends TikaIndexer {
    
    private IndexSchema mySchema;
    
    // FIXME don't copy this code from flume solr sink
    @Override
    protected Map<String, SolrCollection> createSolrCollections() {
      SolrCollection collection = new SolrCollection("default", new MyDocumentLoader());
      try {
        SolrResourceLoader loader = new SolrResourceLoader(solrHomeDir.toString());
        // TODO allow config to be configured by job?
        SolrConfig solrConfig = new SolrConfig(loader, "solrconfig.xml", null);
        mySchema = new IndexSchema(solrConfig, null, null);

        SolrParams params = new MapSolrParams(new HashMap<String,String>());
        Collection<String> dateFormats = DateUtil.DEFAULT_DATE_FORMATS;
        for (PluginInfo pluginInfo : solrConfig.getPluginInfos(SolrRequestHandler.class.getName())) {
          if ("/update/extract".equals(pluginInfo.name)) {
            NamedList initArgs = pluginInfo.initArgs;

            // Copied from StandardRequestHandler
            if (initArgs != null) {
              Object o = initArgs.get("defaults");
              if (o != null && o instanceof NamedList) {
                SolrParams defaults = SolrParams.toSolrParams((NamedList) o);
                params = defaults;
              }
              o = initArgs.get("appends");
              if (o != null && o instanceof NamedList) {
                SolrParams appends = SolrParams.toSolrParams((NamedList) o);
              }
              o = initArgs.get("invariants");
              if (o != null && o instanceof NamedList) {
                SolrParams invariants = SolrParams.toSolrParams((NamedList) o);
              }

              NamedList configDateFormats = (NamedList) initArgs.get(ExtractingRequestHandler.DATE_FORMATS);
              if (configDateFormats != null && configDateFormats.size() > 0) {
                dateFormats = new HashSet<String>();
                Iterator<Map.Entry> it = configDateFormats.iterator();
                while (it.hasNext()) {
                  String format = (String) it.next().getValue();
                  LOG.info("Adding Date Format: {}", format);
                  dateFormats.add(format);
                }
              }
            }
            break; // found it
          }
        }
        collection.setSchema(mySchema);
        collection.setSolrParams(params);
        collection.setDateFormats(dateFormats);

      } catch (SAXException e) {
        throw new ConfigurationException(e);
      } catch (IOException e) {
        throw new ConfigurationException(e);
      } catch (ParserConfigurationException e) {
        throw new ConfigurationException(e);
      }

      return Collections.singletonMap(collection.getName(), collection);
    }

    private class MyDocumentLoader implements DocumentLoader {

      @Override
      public void beginTransaction() {
      }

      @Override
      public void load(List<SolrInputDocument> docs) throws IOException, SolrServerException {
        for (SolrInputDocument doc : docs) {
          String uniqueKeyFieldName = mySchema.getUniqueKeyField().getName();
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
            context.getCounter(TikaCounters.class.getName(), TikaCounters.PARSER_OUTPUT_BYTES.toString()).increment(numParserOutputBytes);
          }
        }
        context.getCounter(TikaCounters.class.getName(), TikaCounters.DOCS_READ.toString()).increment(docs.size());
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
      public UpdateResponse rollback() throws SolrServerException, IOException {
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

}
