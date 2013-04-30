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
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.morphline.SolrMorphlineContext;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.tika.DocumentLoader;
import org.apache.solr.tika.SolrInspector;
import org.apache.solr.tika.SolrServerDocumentLoader;
import org.apache.solr.tika.TestEmbeddedSolrServer;
import org.apache.solr.tika.parser.StreamingAvroContainerParser.ForwardOnlySeekableInputStream;
import org.apache.tika.exception.TikaException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xml.sax.SAXException;

import com.cloudera.cdk.morphline.base.Fields;
import com.cloudera.cdk.morphline.base.Notifications;
import com.cloudera.cdk.morphline.cmd.PipeBuilder;
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
  public void testReadCSV() throws Exception {
    Config config = parse("test-morphlines/readCSV.conf");    
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
  public void testReadLine() throws Exception {
    Config config = parse("test-morphlines/readLine.conf");    
    morphline = createMorphline(config); 
    String path = RESOURCES_DIR + "/test-documents";
    String[] files = new String[] {
      path + "/cars.csv",
    };
    testDocumentTypesInternal(files, expectedRecords);
  }  

  public void testReadMultiLine() throws Exception {
    Config config = parse("test-morphlines/readMultiLine.conf");    
    morphline = createMorphline(config); 
    String path = RESOURCES_DIR + "/test-documents";
    String[] files = new String[] {
      path + "/multiline-stacktrace.log",
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
//        path + "/sample-statuses-20120906-141433.avro",
//        path + "/sample-statuses-20120906-141433",
//        path + "/sample-statuses-20120906-141433.gz",
//        path + "/sample-statuses-20120906-141433.bz2",
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

  @Test
  public void testAvroStringDocuments() throws IOException, SolrServerException, SAXException, TikaException {
    Config config = parse("test-morphlines/readAvroContainer.conf");    
    morphline = createMorphline(config); 
    Schema docSchema = Schema.createRecord("Doc", "adoc", null, false);
    List<Field> docFields = new ArrayList<Field>();   
    Schema itemListSchema = Schema.create(Type.STRING);
    docFields.add(new Field("price", itemListSchema, null, null));
    docSchema.setFields(docFields);    
            
    GenericData.Record document0 = new GenericData.Record(docSchema);
    document0.put("price", str("foo"));   
    
    GenericData.Record document1 = new GenericData.Record(docSchema);
    document1.put("price", str("bar"));   

    ingestAndVerifyAvro(docSchema);
    ingestAndVerifyAvro(docSchema, document0);
    ingestAndVerifyAvro(docSchema, document1);
    ingestAndVerifyAvro(docSchema, document0, document1);
  }

  @Test
  public void testAvroArrayUnionDocument() throws IOException, SolrServerException, SAXException, TikaException {
    Config config = parse("test-morphlines/readAvroContainer.conf");    
    morphline = createMorphline(config); 
    Schema documentSchema = Schema.createRecord("Doc", "adoc", null, false);
    List<Field> docFields = new ArrayList<Field>();   
    Schema intArraySchema = Schema.createArray(Schema.create(Type.INT));
    Schema intArrayUnionSchema = Schema.createUnion(Arrays.asList(intArraySchema, Schema.create(Type.NULL)));
    Schema itemListSchema = Schema.createArray(intArrayUnionSchema);
    docFields.add(new Field("price", itemListSchema, null, null));
    documentSchema.setFields(docFields);        
//    System.out.println(documentSchema.toString(true));
    
//    // create record0
    GenericData.Record document0 = new GenericData.Record(documentSchema);
      document0.put("price", new GenericData.Array(itemListSchema, Arrays.asList(
          new GenericData.Array(intArraySchema, Arrays.asList(1, 2, 3, 4, 5)),
          new GenericData.Array(intArraySchema, Arrays.asList(10, 20)),
          null,
          null,
//          new GenericData.Array(intArraySchema, Arrays.asList()),
          new GenericData.Array(intArraySchema, Arrays.asList(100, 200)),
          null
//          new GenericData.Array(intArraySchema, Arrays.asList(1000))
      )));    

    GenericData.Record document1 = new GenericData.Record(documentSchema);
    document1.put("price", new GenericData.Array(itemListSchema, Arrays.asList(
        new GenericData.Array(intArraySchema, Arrays.asList(1000))
    )));    

    ingestAndVerifyAvro(documentSchema, document0, document1);    
  }
  
  @Test
  public void testAvroComplexDocuments() throws IOException, SolrServerException, SAXException, TikaException {
    Config config = parse("test-morphlines/readAvroContainer.conf");    
    morphline = createMorphline(config); 
    Schema documentSchema = Schema.createRecord("Document", "adoc", null, false);
    List<Field> docFields = new ArrayList<Field>();
    docFields.add(new Field("docId", Schema.create(Type.INT), null, null));
    
      Schema linksSchema = Schema.createRecord("Links", "alink", null, false);
      List<Field> linkFields = new ArrayList<Field>();
      linkFields.add(new Field("backward", Schema.createArray(Schema.create(Type.INT)), null, null));
      linkFields.add(new Field("forward", Schema.createArray(Schema.create(Type.INT)), null, null));
      linksSchema.setFields(linkFields);
      
      docFields.add(new Field("links", Schema.createUnion(Arrays.asList(linksSchema, Schema.create(Type.NULL))), null, null));
//      docFields.add(new Field("links", linksSchema, null, null));
      
      Schema nameSchema = Schema.createRecord("Name", "aname", null, false);
      List<Field> nameFields = new ArrayList<Field>();
      
        Schema languageSchema = Schema.createRecord("Language", "alanguage", null, false);
        List<Field> languageFields = new ArrayList<Field>();
        languageFields.add(new Field("code", Schema.create(Type.STRING), null, null));
//        docFields.add(new Field("links", Schema.createUnion(Arrays.asList(linksSchema, Schema.create(Type.NULL))), null, null));
        languageFields.add(new Field("country", Schema.createUnion(Arrays.asList(Schema.create(Type.STRING), Schema.create(Type.NULL))), null, null));
        languageSchema.setFields(languageFields);
        
      nameFields.add(new Field("language", Schema.createArray(languageSchema), null, null));
      nameFields.add(new Field("url", Schema.createUnion(Arrays.asList(Schema.create(Type.STRING), Schema.create(Type.NULL))), null, null));              
//      nameFields.add(new Field("url", Schema.create(Type.STRING), null, null));             
      nameSchema.setFields(nameFields);
      
    docFields.add(new Field("name", Schema.createArray(nameSchema), null, null));         
    documentSchema.setFields(docFields);    
    
    System.out.println(documentSchema.toString(true));
    
    
    
    // create record0
    GenericData.Record document0 = new GenericData.Record(documentSchema);
    document0.put("docId", 10);
    
    GenericData.Record links = new GenericData.Record(linksSchema);
      links.put("forward", new GenericData.Array(linksSchema.getField("forward").schema(), Arrays.asList(20, 40, 60)));
      links.put("backward", new GenericData.Array(linksSchema.getField("backward").schema(), Arrays.asList()));

    document0.put("links", links);
      
      GenericData.Record name0 = new GenericData.Record(nameSchema);
      
        GenericData.Record language0 = new GenericData.Record(languageSchema);
        language0.put("code", "en-us");
        language0.put("country", "us");
        
        GenericData.Record language1 = new GenericData.Record(languageSchema);
        language1.put("code", "en");
        
      name0.put("language", new GenericData.Array(nameSchema.getField("language").schema(), Arrays.asList(language0, language1)));
      name0.put("url", "http://A");
        
      GenericData.Record name1 = new GenericData.Record(nameSchema);
      name1.put("language", new GenericData.Array(nameSchema.getField("language").schema(), Arrays.asList()));
      name1.put("url", "http://B");
      
      GenericData.Record name2 = new GenericData.Record(nameSchema);
      
      GenericData.Record language2 = new GenericData.Record(languageSchema);
      language2.put("code", "en-gb");
      language2.put("country", "gb");
            
      name2.put("language", new GenericData.Array(nameSchema.getField("language").schema(), Arrays.asList(language2)));     
      
    document0.put("name", new GenericData.Array(documentSchema.getField("name").schema(), Arrays.asList(name0, name1, name2)));     
    System.out.println(document0.toString());

    
    // create record1
    GenericData.Record document1 = new GenericData.Record(documentSchema);
    document1.put("docId", 20);
    
      GenericData.Record links1 = new GenericData.Record(linksSchema);
      links1.put("backward", new GenericData.Array(linksSchema.getField("backward").schema(), Arrays.asList(10, 30)));
      links1.put("forward", new GenericData.Array(linksSchema.getField("forward").schema(), Arrays.asList(80)));

    document1.put("links", links1);
      
      GenericData.Record name4 = new GenericData.Record(nameSchema);      
      name4.put("language", new GenericData.Array(nameSchema.getField("language").schema(), Arrays.asList()));
      name4.put("url", "http://C");
        
    document1.put("name", new GenericData.Array(documentSchema.getField("name").schema(), Arrays.asList(name4)));     
    
    ingestAndVerifyAvro(documentSchema, document0);
//    ingestAndVerifyAvro(documentSchema, document1);
//    ingestAndVerifyAvro(documentSchema, document0, document1);
  }

  private void ingestAndVerifyAvro(Schema schema, GenericData.Record... records) throws IOException, SolrServerException, SAXException, TikaException {
    deleteAllDocuments();
    
    GenericDatumWriter datum = new GenericDatumWriter(schema);
    DataFileWriter writer = new DataFileWriter(datum);
    writer.setMeta("Meta-Key0", "Meta-Value0");
    writer.setMeta("Meta-Key1", "Meta-Value1");
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    writer.create(schema, bout);
    for (GenericData.Record record : records) {
      writer.append(record);
    }
    writer.flush();
    writer.close();

    FileReader<GenericData.Record> reader = new DataFileReader(new ForwardOnlySeekableInputStream(new ByteArrayInputStream(bout.toByteArray())), new GenericDatumReader());
    Schema schema2 = reader.getSchema();
    assertEquals(schema, schema2);
    GenericData.Record record2 = new GenericData.Record(schema2);    
    for (GenericData.Record record : records) {
      assertTrue(reader.hasNext());
      reader.next(record2);
      assertEquals(record, record2);
    }

    Record event = new Record();
    event.getFields().put(Fields.ATTACHMENT_BODY, new ByteArrayInputStream(bout.toByteArray()));
//    StreamEvent event = new StreamEvent(new ByteArrayInputStream(bout.toByteArray()), new HashMap());
    assertTrue(load(event));
    assertEquals(records.length, queryResultSetSize("*:*"));
    
//    deleteAllDocuments();
//    
//    GenericDatumWriter datumWriter = new GenericDatumWriter(schema);
//    bout = new ByteArrayOutputStream();
//    Encoder encoder = EncoderFactory.get().binaryEncoder(bout, null);
//    for (GenericData.Record record : records) {
//      datumWriter.write(record, encoder);
//    }
//    encoder.flush();
//
//    Decoder decoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(bout.toByteArray()), null);
//    DatumReader<Record> datumReader = new GenericDatumReader<Record>(schema);
//    for (int i = 0; i < records.length; i++) {
//      Record record3 = datumReader.read(null, decoder);
//      assertEquals(records[i], record3);
//    }
//  
//    // TODO: clean this up - don't add AvroTestParser to released tika-config.xml
////    event = new StreamEvent(new ByteArrayInputStream(bout.toByteArray()), Collections.singletonMap(ExtractingParams.STREAM_TYPE, AvroTestParser.MEDIA_TYPE.toString()));
//    event = new Record();
//    event.getFields().put(Fields.ATTACHMENT_BODY, new ByteArrayInputStream(bout.toByteArray()));
//    event.getFields().put(Fields.ATTACHMENT_MIME_TYPE, AvroTestParser.MEDIA_TYPE.toString());
//    AvroTestParser.setSchema(schema);
//    load(event);
//    assertEquals(records.length, queryResultSetSize("*:*"));    
  }

  private boolean load(Record record) {
    Notifications.notifyStartSession(morphline);
    return morphline.process(record);
  }
  
  private int queryResultSetSize(String query) {
    return collector.getRecords().size();
  }
  
  private void deleteAllDocuments() {
    collector.reset();
  }
  
  private static Utf8 str(String str) {
    return new Utf8(str);
  }

  protected Command createMorphline(String file) {
    return new PipeBuilder().build(parse(file), null, collector, createMorphlineContext());
  }

  protected Command createMorphline(Config config) {
    return new PipeBuilder().build(config, null, collector, createMorphlineContext());
  }

  private MorphlineContext createMorphlineContext() {
    return new SolrMorphlineContext.Builder()
      .setIndexSchema(schema)
      .setMetricsRegistry(new MetricsRegistry())
      .build();
  }
  
  private Config parse(String file) {
    Config config = Configs.parse(file);
    config = config.getConfigList("morphlines").get(0);
    return config;
  }
  
}
