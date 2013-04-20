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
package com.cloudera.cdk.morphline.api;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.morphline.SolrMorphlineContext;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.tika.DocumentLoader;
import org.apache.solr.tika.SolrInspector;
import org.apache.solr.tika.SolrServerDocumentLoader;
import org.apache.solr.tika.TestEmbeddedSolrServer;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.cloudera.cdk.morphline.base.Connector;
import com.cloudera.cdk.morphline.base.MorphlineBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.yammer.metrics.core.MetricsRegistry;

public class SolrMorphlineTest extends SolrTestCaseJ4 {
  
  private IndexSchema schema;
  private Collector collector;
  private Command morphline;
  
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
  
  protected Map<String, String> getContext() {
    final Map<String, String> context = new HashMap();
    //context.put(TikaIndexer.TIKA_CONFIG_LOCATION, RESOURCES_DIR + "/tika-config.xml");
    context.put(SolrInspector.SOLR_COLLECTION_LIST + ".testcoll." + SolrInspector.SOLR_CLIENT_HOME, testSolrHome + "/collection1");
    return context;
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

    int batchSize = SEQ_NUM2.incrementAndGet() % 2 == 0 ? SolrInspector.DEFAULT_SOLR_SERVER_BATCH_SIZE : 1;
    DocumentLoader testServer = new SolrServerDocumentLoader(solrServer, batchSize);
    Config config = ConfigFactory.parseMap(context);
    schema = new SolrInspector().createSolrCollection(config, testServer).getSchema();
  }
  
  @After
  public void tearDown() throws Exception {
    collector = null;
    super.tearDown();
  }
    
  @Test
  public void testComplexParse() throws Exception {
    parse("testComplexParse-morphline");
  }
  
  @Test
  public void testBasic() throws Exception {
    Config config = parse("testBasic-morphline");    
    morphline = createMorphline(config);    
    Record record = createBasicRecord();
    morphline.startSession();
    morphline.process(record);
    assertEquals(Arrays.asList(record), collector.getRecords());
    assertEquals(1, collector.getNumStartEvents());
  }

  @Test
  public void testBasicFilterPass() throws Exception {
    Config config = parse("testBasicFilterPass-morphline");    
    morphline = createMorphline(config);
    Record record = new Record();
    record.getFields().put("first_name", "Nadja");
    List<Record> expectedList = new ArrayList();
    for (int i = 0; i < 2; i++) {
      Record expected = new Record(record);
      expected.getFields().put("foo", "bar");
      expected.getFields().replaceValues("iter", Arrays.asList(i));
      expectedList.add(expected);
    }
    morphline.startSession();
    morphline.process(record);
    assertEquals(expectedList, collector.getRecords());
    assertTrue(record != collector.getRecords().get(0));
    assertEquals(1, collector.getNumStartEvents());
  }

  @Test
  public void testBasicFilterFail() throws Exception {
    Config config = parse("testBasicFilterFail-morphline");    
    morphline = createMorphline(config);
    Record record = new Record();
    record.getFields().put("first_name", "Nadja");
    List<Record> expectedList = new ArrayList();
    for (int i = 0; i < 2; i++) {
      Record expected = new Record(record);
      expected.getFields().put("foo2", "bar2");
      expected.getFields().replaceValues("iter2", Arrays.asList(i));
      expectedList.add(expected);
    }
    morphline.startSession();
    morphline.process(record);
    assertEquals(expectedList, collector.getRecords());
    assertTrue(record != collector.getRecords().get(0));
    assertEquals(1, collector.getNumStartEvents());
  }
  
  @Test
  public void testBasicFilterFailTwice() throws Exception {
    Config config = parse("testBasicFilterFailTwice-morphline");    
    morphline = createMorphline(config);
    Record record = new Record();
    record.getFields().put("first_name", "Nadja");
    List<Record> expectedList = new ArrayList();
//    for (int i = 0; i < 2; i++) {
//      Record expected = new Record(record);
//      expected.getFields().put("foo2", "bar2");
//      expected.getFields().replaceValues("iter2", Arrays.asList(i));
//      expectedList.add(expected);
//    }
    morphline.startSession();
    try {
      morphline.process(record);
      fail();
    } catch (MorphlineRuntimeException e) {
      assertTrue(e.getMessage().startsWith("Filter found no matching rule"));
    }
    assertEquals(expectedList, collector.getRecords());
    assertEquals(1, collector.getNumStartEvents());
  }
  
  @Test
  public void testJPGCompressed() throws Exception {
    Config config = parse("testJPGCompressed-morphline.conf");    
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
  public void testCSVBasic() throws Exception {
    Config config = parse("testCSVBasic-morphline.conf");    
    morphline = createMorphline(config); 
    String path = RESOURCES_DIR + "/test-documents";
    String[] files = new String[] {
      path + "/cars.csv",
//      path + "/cars.tsv",
//      path + "/cars.ssv",
//      path + "/cars.csv.gz",
//      path + "/cars.tar.gz",
    };
    testDocumentTypesInternal(files, expectedRecords);
  }  

  @Test
  public void testReadLineBasic() throws Exception {
    Config config = parse("testReadLineBasic-morphline.conf");    
    morphline = createMorphline(config); 
    String path = RESOURCES_DIR + "/test-documents";
    String[] files = new String[] {
      path + "/cars.csv",
    };
    testDocumentTypesInternal(files, expectedRecords);
  }  

  public void testMultiLineBasic() throws Exception {
    Config config = parse("testMultiLineBasic-morphline.conf");    
    morphline = createMorphline(config); 
    String path = RESOURCES_DIR + "/test-documents";
    String[] files = new String[] {
      path + "/multiline-stacktrace.log",
    };
    testDocumentTypesInternal(files, expectedRecords);
  }  

  @Test
  public void testDocumentTypes() throws Exception {
    Config config = parse("testDocumentTypes-morphline.conf");    
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
//        path + "/sample-statuses-20120906-141433.avro",
//        path + "/sample-statuses-20120906-141433",
//        path + "/sample-statuses-20120906-141433.gz",
//        path + "/sample-statuses-20120906-141433.bz2",
    };
    testDocumentTypesInternal(files, expectedRecords);
  }
  
  @Test
  public void testDocumentTypes2() throws Exception {
    Config config = parse("testDocumentTypes-morphline.conf");    
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
        path + "/test-documents.cpio",
        path + "/test-documents.tar", 
        path + "/test-documents.tbz2", 
        path + "/test-documents.tgz",
        path + "/test-documents.zip",
        path + "/test-zip-of-zip.zip",
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
//    deleteAllDocuments(solrIndexer);
//    setProductionMode(isProductionMode);
    int numDocs = 0;
    long startTime = System.currentTimeMillis();
    
//    assertEquals(numDocs, queryResultSetSize("*:*", solrIndexer));
//  assertQ(req("*:*"), "//*[@numFound='0']");
    for (int i = 0; i < 1; i++) {
      
      for (String file : files) {
        File f = new File(file);
        byte[] body = FileUtils.readFileToByteArray(f);
        Record event = new Record();
        event.getFields().put(Field.ATTACHMENT_BODY, new ByteArrayInputStream(body));
//        StreamEvent event = new StreamEvent(new ByteArrayInputStream(body), new HashMap());
        event.getFields().put(Field.ATTACHMENT_NAME, f.getName());

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
    morphline.startSession();
    return morphline.process(record);
  }
  
  private int queryResultSetSize(String query) {
    return collector.getRecords().size();
  }
  
  @Test
  @Ignore
  public void testReflection() {
    long start = System.currentTimeMillis();
    List<String> packagePrefixes = Arrays.asList("com", "org", "net");
    for (Class clazz : new ClassPaths().getTopLevelClassesRecursive(
        packagePrefixes, CommandBuilder.class)) {
      System.out.println("found " + clazz);
    }
//    for (Class cmd : new Reflections("com", "org").getSubTypesOf(CommandBuilder.class)) {
//      System.out.println(cmd);
//    }
    float secs = (System.currentTimeMillis() - start) / 1000.0f;
    System.out.println("secs=" + secs);
  }
  
  private Command createMorphline(Config config) {
    return new MorphlineBuilder().build(config, new Connector(), collector, createMorphlineContext());
//  return new Morphline(config, new Connector(), collector, new MorphlineContext());
  }

  private MorphlineContext createMorphlineContext() {
    return new SolrMorphlineContext(new MetricsRegistry(), schema);
  }
  
  private Record createBasicRecord() {
    Record record = new Record();
    record.getFields().put("first_name", "Nadja");
    record.getFields().put("age", 8);
    record.getFields().put("tags", "one");
    record.getFields().put("tags", 2);
    record.getFields().put("tags", "three");
    return record;
  }

  private Config parse(String file) {
    Config config = Configs.parse(file);
    config = config.getConfigList("morphlines").get(0);
    return config;
  }
  
}
