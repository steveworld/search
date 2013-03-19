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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Base type for TikaIndexer tests.
 */
public class TikaIndexerTestBase extends SolrTestCaseJ4 {

  protected SolrIndexer indexer;
  protected boolean injectUnknownSolrField = false; // to force exceptions
  protected boolean injectSolrServerException = false; // force SolrServerException
  protected boolean isProductionMode = false;

  protected static final boolean TEST_WITH_EMBEDDED_SOLR_SERVER = true;
  protected static final String EXTERNAL_SOLR_SERVER_URL = System.getProperty("externalSolrServer");
//  protected static final String EXTERNAL_SOLR_SERVER_URL = "http://127.0.0.1:8983/solr";
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
      //solrServer = new SafeConcurrentUpdateSolrServer(EXTERNAL_SOLR_SERVER_URL, 2, 2);
      solrServer = new HttpSolrServer(EXTERNAL_SOLR_SERVER_URL);
      ((HttpSolrServer)solrServer).setParser(new XMLResponseParser());
    } else {
      if (TEST_WITH_EMBEDDED_SOLR_SERVER) {
        solrServer = new TestEmbeddedSolrServer(h.getCoreContainer(), "");
      } else {
        throw new RuntimeException("Not yet implemented");
        //solrServer = new TestSolrServer(getSolrServer());
      }
    }

    DocumentLoader testServer = new SolrServerDocumentLoader(solrServer);
    Config config = ConfigFactory.parseMap(context);
    indexer = new TikaIndexer(new SolrInspector().createSolrCollection(config, testServer), config) {
      @Override
      public void load(List<SolrInputDocument> docs) throws IOException, SolrServerException {
        for (SolrInputDocument doc : docs) {
          if (injectUnknownSolrField) {
            doc.setField("unknown_bar_field", "baz");
          }
          if (injectSolrServerException) {
            throw new SolrServerException("Injected SolrServerException");
          }
        }
        super.load(docs);
      }
      
      @Override
      protected Config getConfig() {
        Map<String, String> context = getContext();
        if (isProductionMode) {
          context.put(SolrIndexer.PRODUCTION_MODE, "true");
        }
        Config config = ConfigFactory.parseMap(context);
        return config;
      }

    };
    
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

  protected void testDocumentTypesInternal(String[] files, Map<String,Integer> expectedRecords, SolrIndexer solrIndexer, boolean isProductionMode)
  throws Exception {
    deleteAllDocuments(solrIndexer);
    setProductionMode(isProductionMode);
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
        
        if (isProductionMode || !injectUnknownSolrField) {
          load(event, solrIndexer); // must not throw exception
        } else {
          try {
            load(event, solrIndexer); // must throw exception
            fail();
          } catch (Exception e) {
            ; // expected
          }          
        }
        
        if (!injectUnknownSolrField) {
          Integer count = expectedRecords.get(file);
          if (count != null) {
            numDocs += count;
          } else {
            numDocs++;
          }
        }
        assertEquals(numDocs, queryResultSetSize("*:*", solrIndexer));
      }
      LOGGER.trace("iter: {}", i);
    }
    LOGGER.trace("all done with put at {}", System.currentTimeMillis() - startTime);
    assertEquals(numDocs, queryResultSetSize("*:*", solrIndexer));
    LOGGER.trace("indexer: ", solrIndexer);
  }

  protected void testDocumentTypesInternal(String[] files, Map<String,Integer> expectedRecords) throws Exception {
    boolean beforeProd = isProductionMode;
    try {    
      testDocumentTypesInternal(files, expectedRecords, indexer, false);
      testDocumentTypesInternal(files, expectedRecords, indexer, true);
      
      boolean before = injectUnknownSolrField;
      injectUnknownSolrField = true;
      try {
        testDocumentTypesInternal(files, expectedRecords, indexer, false);
        testDocumentTypesInternal(files, expectedRecords, indexer, true);
      } finally {
        injectUnknownSolrField = before;
      }      
      
      testDocumentTypesInternal(files, expectedRecords, indexer, false);
    } finally {
      isProductionMode = beforeProd;
    }
  }

  protected void load(StreamEvent event, SolrIndexer solrIndexer) throws IOException, SolrServerException, SAXException, TikaException {
    if (event instanceof TikaStreamEvent) {
      event = new TikaStreamEvent(event.getBody(), new HashMap(event.getHeaders()), ((TikaStreamEvent)event).getParseContext());
    } else {      
      event = new StreamEvent(event.getBody(), new HashMap(event.getHeaders()));
    }
    event.getHeaders().put("id", "" + SEQ_NUM.getAndIncrement());
    solrIndexer.beginTransaction();
    solrIndexer.process(event);
    solrIndexer.commitTransaction();
  }

  protected void load(StreamEvent event) throws IOException, SolrServerException, SAXException, TikaException {
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
  
  protected void setProductionMode(boolean isProduction) {
    this.isProductionMode = isProduction;
  }
  
}
