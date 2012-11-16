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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.sink.solr.indexer.DocumentLoader;
import org.apache.flume.sink.solr.indexer.ParseInfo;
import org.apache.flume.sink.solr.indexer.SolrCollection;
import org.apache.flume.sink.solr.indexer.StreamEvent;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.hadoop.SolrInputDocumentWritable;
import org.apache.solr.hadoop.SolrMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TikaMapper extends SolrMapper<LongWritable, Text> {
  private static final Logger LOG = LoggerFactory.getLogger(TikaMapper.class);
  
  private final Text id = new Text();

  private MyIndexer indexer;
  private final MyDocumentLoader myDocumentLoader = new MyDocumentLoader();
  private FileSystem fs;
  private Context context;

  private class MyIndexer extends org.apache.flume.sink.solr.indexer.TikaIndexer {
    Map<String, SolrCollection> collections = new LinkedHashMap<String, SolrCollection>();

    @Override
    protected Map<String, SolrCollection> createSolrCollections() {
      SolrCollection collection = new SolrCollection("default", myDocumentLoader);
      collections.put(collection.getName(), collection);
      return collections;
    }
    
  }

  private class MyDocumentLoader implements DocumentLoader {

    @Override
    public void beginTransaction() {
    }

    @Override
    public void load(List<SolrInputDocument> docs) throws IOException, SolrServerException {
      id.set(uniqueId);

      for (SolrInputDocument sid: docs) {
        context.write(id, new SolrInputDocumentWritable(sid));
      }
    }

    @Override
    public void commitTransaction() {
    }

    @Override
    public UpdateResponse rollback() throws SolrServerException, IOException {
      return null;
    }

    @Override
    public void shutdown() {
    }

    @Override
    public SolrPingResponse ping() throws SolrServerException, IOException {
      return null;
    }
    
  }

  @Override
  protected void setup(Context context) throws IOException,
      InterruptedException {
    super.setup(context);
    indexer = new MyIndexer();
    fs = FileSystem.get(context.getConfiguration());
  }

  /**
   * Extract content from the path specified in the value. Key is useless.
   */
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    this.context = context;

    Path p = new Path(value.toString());
    FSDataInputStream in = fs.open(p);
    try {
      indexer.process(new StreamEvent(in, headers));
    } finally {
      in.close();
    }
  }
}
