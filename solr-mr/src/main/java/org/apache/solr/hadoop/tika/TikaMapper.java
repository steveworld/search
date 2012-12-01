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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

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
import org.apache.solr.schema.SchemaField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TikaMapper extends SolrMapper<LongWritable, Text> {
  private static final Logger LOG = LoggerFactory.getLogger(TikaMapper.class);
  
  private final Text id = new Text();

  private MyIndexer indexer;
  private FileSystem fs;
  private Context context;
  private IndexSchema schema;

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

    // FIXME don't copy this code from flume solr sink
    @Override
    public void load(List<SolrInputDocument> docs, String collectionName) throws IOException, SolrServerException {
      SolrCollection coll = getSolrCollections().get(collectionName);
      assert coll != null;
      AtomicLong numRecords = getParseInfo().getRecordNumber();
      for (SolrInputDocument doc : docs) {
        long num = numRecords.getAndIncrement();
        // LOGGER.debug("record #{} loading before doc: {}", num, doc);
        SchemaField uniqueKey = coll.getSchema().getUniqueKeyField();
        if (uniqueKey != null && !doc.containsKey(uniqueKey.getName())) {
          // FIXME handle missing unique field properly
          doc.setField(uniqueKey.getName(), UUID.randomUUID().toString());
        }
        LOG.debug("record #{} loading doc: {}", num, doc);
      }
      getSolrCollections().get(collectionName).getDocumentLoader().load(docs);
    }

  }

  private class MyDocumentLoader implements DocumentLoader {

    @Override
    public void beginTransaction() {
    }

    @Override
    public void load(List<SolrInputDocument> docs) throws IOException, SolrServerException {
      for (SolrInputDocument sid: docs) {
        SchemaField uniqueKeyField = schema.getUniqueKeyField();
        
        id.set(sid.getFieldValue(uniqueKeyField.getName()).toString());

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

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    this.context = context;
    indexer = new MyIndexer();
    HashMap<String, Object> params = new HashMap<String,Object>();
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
      return;
    }
    FSDataInputStream in = fs.open(path);
    try {
      Map<String,String> headers = new HashMap<String, String>();
//      headers.put(schema.getUniqueKeyField().getName(), key.toString()); // FIXME
      indexer.process(new StreamEvent(in, headers));
    } catch (SolrServerException e) {
      LOG.error("Unable to process event ", e);
    } finally {
      in.close();
    }
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    super.cleanup(context);
    indexer.commitTransaction();
    indexer.stop();
  }

}
