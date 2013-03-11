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
package org.apache.solr.tika;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.tika.metadata.Metadata;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Base type for TikaIndexer tests.
 */
public class TikaIndexerTestBase extends SolrJettyTestBase {

  protected SolrIndexer indexer;

  protected static final boolean TEST_WITH_EMBEDDED_SOLR_SERVER = false;
  protected static final String EXTERNAL_SOLR_SERVER_URL = System.getProperty("externalSolrServer");
//private static final String EXTERNAL_SOLR_SERVER_URL = "http://127.0.0.1:8983/solr";
  protected static final String RESOURCES_DIR = "target/test-classes";
//private static final String RESOURCES_DIR = "src/test/resources";
  protected static final AtomicInteger SEQ_NUM = new AtomicInteger();
  protected static final Logger LOGGER = LoggerFactory.getLogger(TestTikaIndexer.class);

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore(
        RESOURCES_DIR + "/solr/collection1/conf/solrconfig.xml",
        RESOURCES_DIR + "/solr/collection1/conf/schema.xml",
        RESOURCES_DIR + "/solr"
        );
//    createJetty(
//        new File(RESOURCES_DIR + "/solr").getAbsolutePath(),
//        null, //RESOURCES_DIR + "/solr/collection1/conf/solrconfig.xml",
//        null
//        );
  }

  protected Map<String, String> getContext() {
    final Map<String, String> context = new HashMap();
    context.put(TikaIndexer.TIKA_CONFIG_LOCATION, RESOURCES_DIR + "/tika-config.xml");
    context.put(SolrInspector.SOLR_COLLECTION_LIST + ".testcoll." + SolrInspector.SOLR_CLIENT_HOME, RESOURCES_DIR + "/solr/collection1");
    return context;
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    final Map<String, String> context = getContext();
    
    final SolrServer solrServer;
    if (EXTERNAL_SOLR_SERVER_URL != null) {
      //solrServer = new ConcurrentUpdateSolrServer(EXTERNAL_SOLR_SERVER_URL, 2, 2);
      solrServer = new SafeConcurrentUpdateSolrServer(EXTERNAL_SOLR_SERVER_URL, 2, 2);
      //solrServer = new HttpSolrServer(EXTERNAL_SOLR_SERVER_URL);
    } else {
      if (TEST_WITH_EMBEDDED_SOLR_SERVER) {
        solrServer = new TestEmbeddedSolrServer(h.getCoreContainer(), "");
      } else {
        solrServer = new TestSolrServer(getSolrServer());
      }
    }

    DocumentLoader testServer = new SolrServerDocumentLoader(solrServer);
    Config config = ConfigFactory.parseMap(context);
    indexer = new TikaIndexer(new SolrInspector().createSolrCollection(config, testServer), config);
    
    deleteAllDocuments();
  }

  protected void deleteAllDocuments(SolrIndexer solrIndexer) throws SolrServerException, IOException {
    SolrCollection collection = solrIndexer.getSolrCollection();
    SolrServer s = ((SolrServerDocumentLoader)collection.getDocumentLoader()).getSolrServer();
    s.deleteByQuery("*:*"); // delete everything!
    s.commit();
  }

  protected void deleteAllDocuments() throws SolrServerException, IOException {
    deleteAllDocuments(indexer);
  }

  public void tearDown(SolrIndexer solrIndexer) throws Exception {
    try {
      if (solrIndexer != null) {
        solrIndexer.stop();
        solrIndexer = null;
      }
    } finally {
      super.tearDown();
    }
  }

  @After
  @Override
  public void tearDown() throws Exception {
    tearDown(indexer);
  }

  protected void testDocumentTypesInternal(String[] files, Map<String,Integer> expectedRecords, SolrIndexer solrIndexer)
  throws Exception {
    int numDocs = 0;
    long startTime = System.currentTimeMillis();
    
    assertEquals(numDocs, queryResultSetSize("*:*", solrIndexer));
//  assertQ(req("*:*"), "//*[@numFound='0']");
    for (int i = 0; i < 1; i++) {
      
      for (String file : files) {
        File f = new File(file);
        byte[] body = FileUtils.readFileToByteArray(f);
        StreamEvent event = new StreamEvent(new ByteArrayInputStream(body), new HashMap());
        event.getHeaders().put(Metadata.RESOURCE_NAME_KEY, f.getName());
        load(event, solrIndexer);
        Integer count = expectedRecords.get(file);
        if (count != null) {
          numDocs += count;
        } else {
          numDocs++;
        }
        assertEquals(numDocs, queryResultSetSize("*:*", solrIndexer));
      }
      LOGGER.trace("iter: {}", i);
    }
    LOGGER.trace("all done with put at {}", System.currentTimeMillis() - startTime);
    assertEquals(numDocs, queryResultSetSize("*:*", solrIndexer));
    LOGGER.trace("indexer: ", solrIndexer);
  }

  protected void testDocumentTypesInternal(String[] files, Map<String,Integer> expectedRecords)
  throws Exception {
    testDocumentTypesInternal(files, expectedRecords, indexer);
  }

  protected void load(StreamEvent event, SolrIndexer solrIndexer) throws IOException, SolrServerException {
    event = new StreamEvent(event.getBody(), new HashMap(event.getHeaders()));
    event.getHeaders().put("id", "" + SEQ_NUM.getAndIncrement());
    solrIndexer.process(event);
  }

  protected void load(StreamEvent event) throws IOException, SolrServerException {
    load(event, indexer);
  }

  private void commit(SolrIndexer solrIndexer) throws SolrServerException, IOException {
    SolrCollection collection = solrIndexer.getSolrCollection();
    ((SolrServerDocumentLoader)collection.getDocumentLoader()).getSolrServer().commit(false, true, true);
  }

  protected int queryResultSetSize(String query, SolrIndexer solrIndexer) throws SolrServerException, IOException {
    commit(solrIndexer);
    SolrCollection collection = solrIndexer.getSolrCollection();
    QueryResponse rsp = ((SolrServerDocumentLoader)collection.getDocumentLoader()).getSolrServer().query(new SolrQuery(query).setRows(Integer.MAX_VALUE));
    LOGGER.debug("rsp: {}", rsp);
    int size = rsp.getResults().size();
    return size;
  }

  protected int queryResultSetSize(String query) throws SolrServerException, IOException {
    return queryResultSetSize(query, indexer);
  }
}
