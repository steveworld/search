/**
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
package org.apache.solr.morphline;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.DateUtil;
import org.apache.solr.handler.extraction.SolrContentHandler;
import org.apache.solr.morphline.solrcell.StripNonCharSolrContentHandlerFactory;
import org.apache.solr.schema.IndexSchema;
import org.apache.tika.metadata.Metadata;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.cdk.morphline.api.Collector;
import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.Compiler;
import com.cloudera.cdk.morphline.base.Fields;
import com.cloudera.cdk.morphline.base.Notifications;
import com.cloudera.cdk.morphline.stdlib.PipeBuilder;
import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.yammer.metrics.core.MetricsRegistry;

public class SolrMorphlineTest extends SolrTestCaseJ4 {
  
  private Collector collector;
  private Command morphline;
  private SolrServer solrServer;
  private DocumentLoader testServer;
  
  protected boolean injectUnknownSolrField = false; // to force exceptions
  protected boolean injectSolrServerException = false; // force SolrServerException
  protected boolean isProductionMode = false;

  private Map<String,Integer> expectedRecords = new HashMap();

  protected static final boolean TEST_WITH_EMBEDDED_SOLR_SERVER = true;
  protected static final String EXTERNAL_SOLR_SERVER_URL = System.getProperty("externalSolrServer");
//  protected static final String EXTERNAL_SOLR_SERVER_URL = "http://127.0.0.1:8983/solr";

  protected static final String RESOURCES_DIR = "target/test-classes";
  protected static final String DEFAULT_BASE_DIR = "solr";
  protected static final AtomicInteger SEQ_NUM = new AtomicInteger();
  protected static final AtomicInteger SEQ_NUM2 = new AtomicInteger();
  
  private static final Logger LOGGER = LoggerFactory.getLogger(SolrMorphlineTest.class);

  @BeforeClass
  public static void beforeClass() throws Exception {
    myInitCore(DEFAULT_BASE_DIR);
  }

  protected static void myInitCore(String baseDirName) throws Exception {
    initCore(
        RESOURCES_DIR + "/" + baseDirName + "/collection1/conf/solrconfig.xml",
        RESOURCES_DIR + "/" + baseDirName + "/collection1/conf/schema.xml",
        RESOURCES_DIR + "/" + baseDirName
        );    
  }
  
  @Before
  public void setUp() throws Exception {
    super.setUp();
    collector = new Collector();
    
    String path = RESOURCES_DIR + "/test-documents";
    expectedRecords.put(path + "/sample-statuses-20120906-141433.avro", 2);
    expectedRecords.put(path + "/sample-statuses-20120906-141433", 2);
    expectedRecords.put(path + "/sample-statuses-20120906-141433.gz", 2);
    expectedRecords.put(path + "/sample-statuses-20120906-141433.bz2", 2);
    expectedRecords.put(path + "/cars.csv", 5);
    expectedRecords.put(path + "/cars.csv.gz", 5);
    expectedRecords.put(path + "/cars.tar.gz", 4);
    expectedRecords.put(path + "/cars.tsv", 5);
    expectedRecords.put(path + "/cars.ssv", 5);
    expectedRecords.put(path + "/test-documents.7z", 9);
    expectedRecords.put(path + "/test-documents.cpio", 9);
    expectedRecords.put(path + "/test-documents.tar", 9);
    expectedRecords.put(path + "/test-documents.tbz2", 9);
    expectedRecords.put(path + "/test-documents.tgz", 9);
    expectedRecords.put(path + "/test-documents.zip", 9);
    expectedRecords.put(path + "/multiline-stacktrace.log", 4);
    
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

    int batchSize = SEQ_NUM2.incrementAndGet() % 2 == 0 ? 100 : 1; //SolrInspector.DEFAULT_SOLR_SERVER_BATCH_SIZE : 1;
    testServer = new SolrServerDocumentLoader(solrServer, batchSize);
    deleteAllDocuments();
  }
  
  @After
  public void tearDown() throws Exception {
    collector = null;
    solrServer = null;
    super.tearDown();
  }
  
  @Test
  public void testLoadSolrBasic() throws Exception {
    //System.setProperty("ENV_SOLR_HOME", testSolrHome + "/collection1");
    Config config = parse("test-morphlines/loadSolrBasic.conf");    
    //System.clearProperty("ENV_SOLR_HOME");
    morphline = createMorphline(config);
    Record record = new Record();
    record.put(Fields.ID, "id0");
    record.put("first_name", "Nadja"); // will be sanitized
    startSession();
    Notifications.notifyBeginTransaction(morphline);
    assertTrue(morphline.process(record));
    assertEquals(1, collector.getNumStartEvents());
    Notifications.notifyCommitTransaction(morphline);
    Record expected = new Record();
    expected.put(Fields.ID, "id0");
    assertEquals(Arrays.asList(expected), collector.getRecords());
    assertEquals(1, queryResultSetSize("*:*"));
    Notifications.notifyRollbackTransaction(morphline);
    Notifications.notifyShutdown(morphline);
  }
    
  @Test
  public void testSolrCellJPGCompressed() throws Exception {
    Config config = parse("test-morphlines/solrCellJPGCompressed.conf");    
    morphline = createMorphline(config); 
    String path = RESOURCES_DIR + "/test-documents";
    String[] files = new String[] {
        path + "/testJPEG_EXIF.jpg",
        path + "/testJPEG_EXIF.jpg.gz",
        path + "/testJPEG_EXIF.jpg.tar.gz",
    };
    testDocumentTypesInternal(files, expectedRecords);
  }  

  @Test
  public void testSolrCellDocumentTypes() throws Exception {
    Config config = parse("test-morphlines/solrCellDocumentTypes.conf");    
    morphline = createMorphline(config); 
    String path = RESOURCES_DIR + "/test-documents";
    String[] files = new String[] {
        path + "/testBMPfp.txt",
        path + "/boilerplate.html",
        path + "/NullHeader.docx",
        path + "/testWORD_various.doc",          
        path + "/testPDF.pdf",
        path + "/testJPEG_EXIF.jpg",
        path + "/testJPEG_EXIF.jpg.gz",
        path + "/testJPEG_EXIF.jpg.tar.gz",
        path + "/testXML.xml",          
//        path + "/cars.csv",
//        path + "/cars.tsv",
//        path + "/cars.ssv",
//        path + "/cars.csv.gz",
//        path + "/cars.tar.gz",
        path + "/sample-statuses-20120906-141433.avro",
        path + "/sample-statuses-20120906-141433",
        path + "/sample-statuses-20120906-141433.gz",
        path + "/sample-statuses-20120906-141433.bz2",
    };
    testDocumentTypesInternal(files, expectedRecords);
  }
  
  @Test
  public void testSolrCellDocumentTypes2() throws Exception {
    Config config = parse("test-morphlines/solrCellDocumentTypes.conf");    
    morphline = createMorphline(config); 
    String path = RESOURCES_DIR + "/test-documents";
    String[] files = new String[] {
        path + "/testPPT_various.ppt",
        path + "/testPPT_various.pptx",        
        path + "/testEXCEL.xlsx",
        path + "/testEXCEL.xls", 
        path + "/testPages.pages", 
        path + "/testNumbers.numbers", 
        path + "/testKeynote.key",
        
        path + "/testRTFVarious.rtf", 
        path + "/complex.mbox", 
        path + "/test-outlook.msg", 
        path + "/testEMLX.emlx",
//        path + "/testRFC822",  
        path + "/rsstest.rss", 
//        path + "/testDITA.dita", 
        
        path + "/testMP3i18n.mp3", 
        path + "/testAIFF.aif", 
        path + "/testFLAC.flac", 
//        path + "/testFLAC.oga", 
//        path + "/testVORBIS.ogg",  
        path + "/testMP4.m4a", 
        path + "/testWAV.wav", 
//        path + "/testWMA.wma", 
        
        path + "/testFLV.flv", 
//        path + "/testWMV.wmv", 
        
        path + "/testBMP.bmp", 
        path + "/testPNG.png", 
        path + "/testPSD.psd",        
        path + "/testSVG.svg",  
        path + "/testTIFF.tif",     

//        path + "/test-documents.7z", 
//        path + "/test-documents.cpio",
//        path + "/test-documents.tar", 
//        path + "/test-documents.tbz2", 
//        path + "/test-documents.tgz",
//        path + "/test-documents.zip",
//        path + "/test-zip-of-zip.zip",
//        path + "/testJAR.jar",
        
//        path + "/testKML.kml", 
//        path + "/testRDF.rdf", 
        path + "/testTrueType.ttf", 
        path + "/testVISIO.vsd",
//        path + "/testWAR.war", 
//        path + "/testWindows-x86-32.exe",
//        path + "/testWINMAIL.dat", 
//        path + "/testWMF.wmf", 
    };   
    testDocumentTypesInternal(files, expectedRecords);
  }

  protected void testDocumentTypesInternal(String[] files, Map<String,Integer> expectedRecords) throws Exception {
    testDocumentTypesInternal(files, expectedRecords, false);
//    boolean beforeProd = isProductionMode;
//    try {    
//      testDocumentTypesInternal(files, expectedRecords, indexer, false);
//      testDocumentTypesInternal(files, expectedRecords, indexer, true);
//      
//      boolean before = injectUnknownSolrField;
//      injectUnknownSolrField = true;
//      try {
//        testDocumentTypesInternal(files, expectedRecords, indexer, false);
//        testDocumentTypesInternal(files, expectedRecords, indexer, true);
//      } finally {
//        injectUnknownSolrField = before;
//      }      
//      
//      testDocumentTypesInternal(files, expectedRecords, indexer, false);
//    } finally {
//      isProductionMode = beforeProd;
//    }
  }

//  protected void testDocumentTypesInternal(String[] files, Map<String,Integer> expectedRecords, SolrIndexer solrIndexer, boolean isProductionMode)
  protected void testDocumentTypesInternal(String[] files, Map<String,Integer> expectedRecords, boolean isProductionMode)
  throws Exception {
    deleteAllDocuments();
//    deleteAllDocuments(solrIndexer);
//    setProductionMode(isProductionMode);
    int numDocs = 0;
    int docId = 0;
    long startTime = System.currentTimeMillis();
    
//    assertEquals(numDocs, queryResultSetSize("*:*", solrIndexer));
//  assertQ(req("*:*"), "//*[@numFound='0']");
    for (int i = 0; i < 1; i++) {
      
      for (String file : files) {
        File f = new File(file);
        byte[] body = Files.toByteArray(f);
        Record event = new Record();
        event.put(Fields.ID, docId++);
        event.getFields().put(Fields.ATTACHMENT_BODY, new ByteArrayInputStream(body));
//        StreamEvent event = new StreamEvent(new ByteArrayInputStream(body), new HashMap());
        event.getFields().put(Fields.ATTACHMENT_NAME, f.getName());

        boolean injectUnknownSolrField = false;

        if (isProductionMode || !injectUnknownSolrField) {
          load(event); // must not throw exception
//          load(event, solrIndexer); // must not throw exception
        } else {
          try {
//            load(event, solrIndexer); // must throw exception
            load(event); // must throw exception
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
        assertEquals(numDocs, queryResultSetSize("*:*"));
        //assertEquals(numDocs, queryResultSetSize("*:*", solrIndexer));
      }
//      LOGGER.trace("iter: {}", i);
    }
//    LOGGER.trace("all done with put at {}", System.currentTimeMillis() - startTime);
    assertEquals(numDocs, queryResultSetSize("*:*"));
//    assertEquals(numDocs, queryResultSetSize("*:*", solrIndexer));
//    LOGGER.trace("indexer: ", solrIndexer);
  }

  private boolean load(Record record) {
    Notifications.notifyStartSession(morphline);
    return morphline.process(record);
  }
  
  private int queryResultSetSize(String query) {
//    return collector.getRecords().size();
    try {
      testServer.commitTransaction();
      solrServer.commit(false, true, true);
      QueryResponse rsp = solrServer.query(new SolrQuery(query).setRows(Integer.MAX_VALUE));
      LOGGER.debug("rsp: {}", rsp);
      int i = 0;
      for (SolrDocument doc : rsp.getResults()) {
        LOGGER.debug("rspDoc #{}: {}", i++, doc);
      }
      int size = rsp.getResults().size();
      return size;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  private void deleteAllDocuments() throws SolrServerException, IOException {
    collector.reset();
    SolrServer s = solrServer;
    s.deleteByQuery("*:*"); // delete everything!
    s.commit();
  }

  protected Command createMorphline(String file) {
    return new PipeBuilder().build(parse(file), null, collector, createMorphlineContext());
  }

  protected Command createMorphline(Config config) {
    return new PipeBuilder().build(config, null, collector, createMorphlineContext());
  }

  private MorphlineContext createMorphlineContext() {
    return new SolrMorphlineContext.Builder()
      .setDocumentLoader(testServer)
//      .setDocumentLoader(new CollectingDocumentLoader(100))
      .setFaultTolerance(new FaultTolerance(false,  false))
      .setMetricsRegistry(new MetricsRegistry())
      .build();
  }
  
  private Config parse(String file) {
    SolrLocator locator = new SolrLocator(createMorphlineContext());
    locator.setSolrHomeDir(testSolrHome + "/collection1");
    Config config = new Compiler().parse(file, locator.toConfig("SOLR_LOCATOR"));
    config = config.getConfigList("morphlines").get(0);
    return config;
  }
  
  protected void startSession() {
    Notifications.notifyStartSession(morphline);
  }
  
  /**
   * Test that the ContentHandler properly strips the illegal characters
   */
  @Test
  public void testTransformValue() {
    String fieldName = "user_name";
    assertFalse("foobar".equals(getFoobarWithNonChars()));

    Metadata metadata = new Metadata();
    // load illegal char string into a metadata field and generate a new document,
    // which will cause the ContentHandler to be invoked.
    metadata.set(fieldName, getFoobarWithNonChars());
    StripNonCharSolrContentHandlerFactory contentHandlerFactory =
      new StripNonCharSolrContentHandlerFactory(DateUtil.DEFAULT_DATE_FORMATS);
    IndexSchema schema = h.getCore().getSchema();
    SolrContentHandler contentHandler =
      contentHandlerFactory.createSolrContentHandler(metadata, new MapSolrParams(new HashMap()), schema);
    SolrInputDocument doc = contentHandler.newDocument();
    String foobar = doc.getFieldValue(fieldName).toString();
    assertTrue("foobar".equals(foobar));
  }

  /**
   * Returns string "foobar" with illegal characters interspersed.
   */
  private String getFoobarWithNonChars() {
    char illegalChar = '\uffff';
    StringBuilder builder = new StringBuilder();
    builder.append(illegalChar).append(illegalChar).append("foo").append(illegalChar)
      .append(illegalChar).append("bar").append(illegalChar).append(illegalChar);
    return builder.toString();
  }

}
