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

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.flume.sink.solr.indexer.Configuration;
import org.apache.flume.sink.solr.indexer.ConfigurationException;
import org.apache.flume.sink.solr.indexer.DocumentLoader;
import org.apache.flume.sink.solr.indexer.SolrCollection;
import org.apache.flume.sink.solr.indexer.StreamEvent;
import org.apache.flume.sink.solr.indexer.TikaIndexer;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.hadoop.SolrInputDocumentWritable;
import org.apache.solr.hadoop.SolrMapper;
import org.apache.solr.schema.IndexSchema;
import org.apache.tika.metadata.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TikaMapper extends SolrMapper<LongWritable, Text> {

  private MyIndexer indexer;
  private FileSystem fs;
  private Context context;
  private IndexSchema schema;

  public static final String SCHEMA_FIELD_NAME_OF_FILE_URI = "fileURI";
  
  private static final Logger LOG = LoggerFactory.getLogger(TikaMapper.class);
  
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    this.context = context;
    indexer = new MyIndexer();
    Map<String, Object> params = new HashMap<String,Object>();
    params.put(TikaIndexer.TIKA_CONFIG_LOCATION, "tika-config.xml");
    Config config = ConfigFactory.parseMap(params);
    indexer.configure(new Configuration(config));
    indexer.start();
    indexer.beginTransaction();
    fs = FileSystem.get(context.getConfiguration());
  }

  /**
   * Extract content from the path specified in the value. Key is useless.
   */
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    Path path = new Path(value.toString());
    if (!fs.exists(path)) {
      return; // ignore files that somehow have been deleted since the job was submitted
    }
    FSDataInputStream in = fs.open(path);
    try {
      Map<String,String> headers = new HashMap<String, String>();
      String uri = getFileURI(path);   
      headers.put(schema.getUniqueKeyField().getName(), uri); // use HDFS file path as docId if no docId is specified
      headers.put(SCHEMA_FIELD_NAME_OF_FILE_URI, uri); // enable explicit storing of path in Solr
      headers.put(Metadata.RESOURCE_NAME_KEY, path.getName()); // Tika can use the file name in guessing the right MIME type
      indexer.process(new StreamEvent(in, headers));
    } catch (SolrServerException e) {
      LOG.error("Unable to process event ", e);
    } finally {
      in.close();
    }
  }

  // TODO: figure out best approach, also consider escaping issues
  private String getFileURI(Path path) {
    return path.toString();
    
//    URI uri = path.toUri();
//    String scheme =  uri.getScheme();
//    if (scheme == null) {
//      scheme = fs.getScheme();
//    }
//
//    String authority = uri.getAuthority();
//    if (authority == null) {
//      authority = "";
//    }
//    if (true) {
//      return scheme + "://" + authority + path.toUri().getPath();
//    } else {    
//      // omit URI authority because name node host may change over time. 
//      // On the other hand this implies that only one HDFS system can be indexed.
//      return scheme + "://" + path.toUri().getPath();
//    }
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    super.cleanup(context);
    indexer.commitTransaction();
    indexer.stop();
  }

  private class MyIndexer extends TikaIndexer {
    @Override
    protected Map<String, SolrCollection> createSolrCollections() {
      SolrCollection collection = new SolrCollection("default", new MyDocumentLoader());
      try {
        SolrResourceLoader loader = new SolrResourceLoader(solrHomeDir.toString());
        // TODO allow config to be configured by job?
        SolrConfig solrConfig = new SolrConfig(loader, "solrconfig.xml", null);
        schema = new IndexSchema(solrConfig, null, null);
        collection.setSchema(schema);
        SolrParams params = new MapSolrParams(new HashMap<String,String>());
        collection.setSolrParams(params);
      } catch (SAXException e) {
        throw new ConfigurationException(e);
      } catch (IOException e) {
        throw new ConfigurationException(e);
      } catch (ParserConfigurationException e) {
        throw new ConfigurationException(e);
      }
      return Collections.singletonMap(collection.getName(), collection);
    }

  }

  private class MyDocumentLoader implements DocumentLoader {

    @Override
    public void beginTransaction() {
    }

    @Override
    public void load(List<SolrInputDocument> docs) throws IOException, SolrServerException {
      for (SolrInputDocument sid: docs) {
        Text id = new Text(sid.getFieldValue(schema.getUniqueKeyField().getName()).toString());
        try {
          context.write(id, new SolrInputDocumentWritable(sid));
        } catch (InterruptedException e) {
          throw new IOException("Interrupted while writing " + sid, e);
        }
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
