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
package org.apache.flume.sink.solr.indexer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.FileUtils;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.sink.solr.SolrSink;
import org.apache.flume.sink.solr.UUIDInterceptor;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.handler.extraction.ExtractingParams;
import org.apache.tika.metadata.Metadata;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestTikaSolrSink extends SolrJettyTestBase {

  private EmbeddedSource source;
  private SolrSink sink;

  private static final boolean TEST_WITH_EMBEDDED_SOLR_SERVER = false;
  private static final String EXTERNAL_SOLR_SERVER_URL = System.getProperty("externalSolrServer");
//private static final String EXTERNAL_SOLR_SERVER_URL = "http://127.0.0.1:8983/solr";
  private static final String RESOURCES_DIR = "target/test-classes";
//private static final String RESOURCES_DIR = "src/test/resources";
  private static final AtomicInteger SEQ_NUM = new AtomicInteger();
  private static final Logger LOGGER = LoggerFactory.getLogger(TestTikaSolrSink.class);

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

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    final Map<String, String> context = new HashMap();
    context.put(TikaIndexer.TIKA_CONFIG_LOCATION, "src/test/resources/tika-config.xml");
    context.put(TikaIndexer.SOLR_COLLECTION_LIST + ".testcoll." + TikaIndexer.SOLR_CLIENT_HOME, RESOURCES_DIR + "/solr/collection1");
    
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

    Map<String, String> channelContext = new HashMap();
    channelContext.put("capacity", "1000000");
    channelContext.put("keep-alive", "0"); // for faster tests
    Channel channel = new MemoryChannel();
    channel.setName(channel.getClass().getName() + SEQ_NUM.getAndIncrement());
    Configurables.configure(channel, new Context(channelContext));
 
    class MySolrSink extends SolrSink {
      public MySolrSink(TikaIndexer indexer) {
        super(indexer);
      }
    }
    sink = new MySolrSink(new TikaIndexer() {
        @Override
        protected List<DocumentLoader> createTestSolrServers() {
          return Collections.singletonList((DocumentLoader) new SolrServerDocumentLoader(solrServer));
        }
      }
    );
    sink.setName(sink.getClass().getName() + SEQ_NUM.getAndIncrement());
    sink.configure(new Context(context));
    sink.setChannel(channel);
    sink.start();
    
    source = new EmbeddedSource(sink);    
    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(Collections.singletonList(channel));
    ChannelProcessor chp = new ChannelProcessor(rcs);
    Context chpContext = new Context();
    chpContext.put("interceptors", "uuidinterceptor");
    chpContext.put("interceptors.uuidinterceptor.type", UUIDInterceptor.Builder.class.getName());
    chp.configure(chpContext);
    source.setChannelProcessor(chp);
    
    deleteAllDocuments();
  }
  
  private void deleteAllDocuments() throws SolrServerException, IOException {
    for (SolrCollection collection : sink.getIndexer().getSolrCollections().values()) {
      SolrServer s = ((SolrServerDocumentLoader)collection.getDocumentLoader()).getSolrServer();
      s.deleteByQuery("*:*"); // delete everything!
      s.commit();
    }
  }

  @After
  @Override
  public void tearDown() throws Exception {
    try {
      if (source != null) {
        source.stop();
        source = null;
      }
      if (sink != null) {
        sink.stop();
        sink = null;
      }
    } finally {
      super.tearDown();
    }
  }

  @Test
  public void testDocumentTypes() throws Exception {
    String path = RESOURCES_DIR + "/test-documents";
    String[] files = new String[] {
        path + "/testBMPfp.txt",
        path + "/boilerplate.html",
        path + "/NullHeader.docx",
        path + "/testWORD_various.doc",          
        path + "/testPDF.pdf",
        path + "/testJPEG_EXIF.jpg",
        path + "/testXML.xml",          
        path + "/cars.csv",
        path + "/cars.csv.gz",
        path + "/cars.tar.gz",
        path + "/sample-statuses-20120906-141433.avro",
    };
    testDocumentTypesInternal(files);
  }

  @Test
  public void testDocumentTypes2() throws Exception {
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
        path + "/testRFC822",  
        path + "/rsstest.rss", 
        path + "/testDITA.dita", 
        
//        path + "/testMP3i18n.mp3", 
//        path + "/testAIFF.aif", 
//        path + "/testFLAC.flac", 
////        path + "/testFLAC.oga", 
//        path + "/testVORBIS.ogg",  
//        path + "/testMP4.m4a", 
//        path + "/testWAV.wav", 
//        path + "/testWMA.wma", 
        
        path + "/testFLV.flv", 
        path + "/testWMV.wmv", 
        
        path + "/testBMP.bmp", 
        path + "/testPNG.png", 
        path + "/testPSD.psd",        
        path + "/testSVG.svg",  
        path + "/testTIFF.tif",     

        path + "/test-documents.7z", 
        path + "/test-documents.cpio",
        path + "/test-documents.tar", 
        path + "/test-documents.tbz2", 
        path + "/test-documents.tgz",
        path + "/test-documents.zip",
        path + "/test-zip-of-zip.zip",
        path + "/testJAR.jar",
        
//        path + "/testKML.kml", 
//        path + "/testRDF.rdf", 
//        path + "/testTrueType.ttf", 
//        path + "/testVISIO.vsd",
//        path + "/testWAR.war", 
////        path + "/testWindows-x86-32.exe",
//        path + "/testWINMAIL.dat", 
//        path + "/testWMF.wmf", 
    };   
    testDocumentTypesInternal(files);
  }

  private void testDocumentTypesInternal(String[] files) throws Exception {
    int numDocs = 0;
    long startTime = System.currentTimeMillis();
    
    assertEquals(numDocs, queryResultSetSize("*:*"));      
//  assertQ(req("*:*"), "//*[@numFound='0']");
    for (int i = 0; i < 1; i++) {
      String path = RESOURCES_DIR + "/test-documents";
      Map<String,Integer> numRecords = new HashMap();
      numRecords.put(path + "/sample-statuses-20120906-141433.avro", 2);
      numRecords.put(path + "/cars.csv", 5);
      numRecords.put(path + "/cars.csv.gz", 5);
      numRecords.put(path + "/cars.tar.gz", 4);
      
      for (String file : files) {
        File f = new File(file);
        byte[] body = FileUtils.readFileToByteArray(f);
        Event event = EventBuilder.withBody(body);
        event.getHeaders().put(Metadata.RESOURCE_NAME_KEY, f.getName());
        load(event);
        Integer count = numRecords.get(file);
        if (count != null) {
          numDocs += count;
        } else {
          numDocs++;
        }
        assertEquals(numDocs, queryResultSetSize("*:*"));
      }
      LOGGER.trace("iter: {}", i);
    }
    LOGGER.trace("all done with put at {}", System.currentTimeMillis() - startTime);
    assertEquals(numDocs, queryResultSetSize("*:*"));
    LOGGER.trace("sink: ", sink);
  }

//  @Test
  public void benchmarkDocumentTypes() throws Exception {
    int iters = 200;
    
    LogManager.getLogger(getClass().getPackage().getName()).setLevel(Level.INFO);
    
    assertEquals(0, queryResultSetSize("*:*"));      
    String path = RESOURCES_DIR + "/test-documents";
    String[] files = new String[] {
//        path + "/testBMPfp.txt",
//        path + "/boilerplate.html",
//        path + "/NullHeader.docx",
//        path + "/testWORD_various.doc",          
//        path + "/testPDF.pdf",
//        path + "/testJPEG_EXIF.jpg",
//        path + "/testXML.xml",          
//        path + "/cars.csv",
//        path + "/cars.csv.gz",
//        path + "/cars.tar.gz",
//        path + "/sample-statuses-20120906-141433.avro",
        path + "/sample-statuses-20120906-141433-medium.avro",
    };
    
    List<Event> events = new ArrayList();
    for (String file : files) {
      File f = new File(file);
      byte[] body = FileUtils.readFileToByteArray(f);
      Event event = EventBuilder.withBody(body);
//      event.getHeaders().put(Metadata.RESOURCE_NAME_KEY, f.getName());
      events.add(event);
    }
    
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < iters; i++) {
      if (i % 10000 == 0) {
        LOGGER.info("iter: {}", i);
      }
      for (Event event : events) {
        event = EventBuilder.withBody(event.getBody(), new HashMap(event.getHeaders()));
        event.getHeaders().put("id", UUID.randomUUID().toString());
        load(event);
      }
    }
    
    float secs = (System.currentTimeMillis() - startTime) / 1000.0f;
    long numDocs = queryResultSetSize("*:*");
    LOGGER.info("Took secs: " + secs + ", iters/sec: " + (iters/secs));
    LOGGER.info("Took secs: " + secs + ", docs/sec: " + (numDocs/secs));
    LOGGER.info("Iterations: " + iters + ", numDocs: " + numDocs);
    LOGGER.info("sink: ", sink);
  }

  @Test
  public void testAvroStringDocuments() throws IOException, EventDeliveryException, SolrServerException {
    Schema docSchema = Schema.createRecord("Doc", "adoc", null, false);
    List<Field> docFields = new ArrayList<Field>();   
    Schema itemListSchema = Schema.create(Type.STRING);
    docFields.add(new Field("price", itemListSchema, null, null));
    docSchema.setFields(docFields);    
            
    Record document0 = new Record(docSchema);
    document0.put("price", str("foo"));   
    
    Record document1 = new Record(docSchema);
    document1.put("price", str("bar"));   

    ingestAndVerifyAvro(docSchema);
    ingestAndVerifyAvro(docSchema, document0);
    ingestAndVerifyAvro(docSchema, document1);
    ingestAndVerifyAvro(docSchema, document0, document1);
  }

  @Test
  public void testAvroArrayUnionDocument() throws IOException, EventDeliveryException, SolrServerException {
    Schema documentSchema = Schema.createRecord("Doc", "adoc", null, false);
    List<Field> docFields = new ArrayList<Field>();   
    Schema intArraySchema = Schema.createArray(Schema.create(Type.INT));
    Schema intArrayUnionSchema = Schema.createUnion(Arrays.asList(intArraySchema, Schema.create(Type.NULL)));
    Schema itemListSchema = Schema.createArray(intArrayUnionSchema);
    docFields.add(new Field("price", itemListSchema, null, null));
    documentSchema.setFields(docFields);        
//    System.out.println(documentSchema.toString(true));
    
//    // create record0
    Record document0 = new Record(documentSchema);
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

    Record document1 = new Record(documentSchema);
    document1.put("price", new GenericData.Array(itemListSchema, Arrays.asList(
        new GenericData.Array(intArraySchema, Arrays.asList(1000))
    )));    

    ingestAndVerifyAvro(documentSchema, document0, document1);    
  }
  
  @Test
  public void testAvroComplexDocuments() throws IOException, EventDeliveryException, SolrServerException {
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
    
//    System.out.println(documentSchema.toString(true));
    
    
    
    // create record0
    Record document0 = new Record(documentSchema);
    document0.put("docId", 10);
    
      Record links = new Record(linksSchema);
      links.put("forward", new GenericData.Array(linksSchema.getField("forward").schema(), Arrays.asList(20, 40, 60)));
      links.put("backward", new GenericData.Array(linksSchema.getField("backward").schema(), Arrays.asList()));

    document0.put("links", links);
      
      Record name0 = new Record(nameSchema);
      
        Record language0 = new Record(languageSchema);
        language0.put("code", "en-us");
        language0.put("country", "us");
        
        Record language1 = new Record(languageSchema);
        language1.put("code", "en");
        
      name0.put("language", new GenericData.Array(nameSchema.getField("language").schema(), Arrays.asList(language0, language1)));
      name0.put("url", "http://A");
        
      Record name1 = new Record(nameSchema);
      name1.put("language", new GenericData.Array(nameSchema.getField("language").schema(), Arrays.asList()));
      name1.put("url", "http://B");
      
      Record name2 = new Record(nameSchema);
      
      Record language2 = new Record(languageSchema);
      language2.put("code", "en-gb");
      language2.put("country", "gb");
            
      name2.put("language", new GenericData.Array(nameSchema.getField("language").schema(), Arrays.asList(language2)));     
      
    document0.put("name", new GenericData.Array(documentSchema.getField("name").schema(), Arrays.asList(name0, name1, name2)));     
//    System.out.println(document0.toString());

    
    // create record1
    Record document1 = new Record(documentSchema);
    document1.put("docId", 20);
    
      Record links1 = new Record(linksSchema);
      links1.put("backward", new GenericData.Array(linksSchema.getField("backward").schema(), Arrays.asList(10, 30)));
      links1.put("forward", new GenericData.Array(linksSchema.getField("forward").schema(), Arrays.asList(80)));

    document1.put("links", links1);
      
      Record name4 = new Record(nameSchema);      
      name4.put("language", new GenericData.Array(nameSchema.getField("language").schema(), Arrays.asList()));
      name4.put("url", "http://C");
        
    document1.put("name", new GenericData.Array(documentSchema.getField("name").schema(), Arrays.asList(name4)));     
    
    ingestAndVerifyAvro(documentSchema, document0);
    ingestAndVerifyAvro(documentSchema, document1);
    ingestAndVerifyAvro(documentSchema, document0, document1);
  }

  private void ingestAndVerifyAvro(Schema schema, Record... records) throws IOException,
      EventDeliveryException, SolrServerException {
    
    deleteAllDocuments();
    
    GenericDatumWriter datum = new GenericDatumWriter(schema);
    DataFileWriter writer = new DataFileWriter(datum);
    writer.setMeta("Meta-Key0", "Meta-Value0");
    writer.setMeta("Meta-Key1", "Meta-Value1");
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    writer.create(schema, bout);
    for (Record record : records) {
      writer.append(record);
    }
    writer.flush();
    writer.close();

    FileReader<Record> reader = DataFileReader.openReader(new SeekableByteArrayInput(bout.toByteArray()), new GenericDatumReader());
    Schema schema2 = reader.getSchema();
    assertEquals(schema, schema2);
    Record record2 = new GenericData.Record(schema2);    
    for (Record record : records) {
      assertTrue(reader.hasNext());
      reader.next(record2);
      assertEquals(record, record2);
    }

    Event event = EventBuilder.withBody(bout.toByteArray());
    load(event);
    assertEquals(records.length, queryResultSetSize("*:*"));
    
    deleteAllDocuments();
    
    GenericDatumWriter datumWriter = new GenericDatumWriter(schema);
    bout = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(bout, null);
    for (Record record : records) {
      datumWriter.write(record, encoder);
    }
    encoder.flush();
    
    Decoder decoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(bout.toByteArray()), null);
    DatumReader<Record> datumReader = new GenericDatumReader<Record>(schema);
    for (int i = 0; i < records.length; i++) {
      Record record3 = datumReader.read(null, decoder);
      assertEquals(records[i], record3);
    }
  
    // TODO: clean this up - don't add AvroTestParser to released tika-config.xml
    event = EventBuilder.withBody(bout.toByteArray(), Collections.singletonMap(ExtractingParams.STREAM_TYPE, AvroTestParser.MEDIA_TYPE));
    AvroTestParser.setSchema(schema);
    load(event);
    assertEquals(records.length, queryResultSetSize("*:*"));    
  }
  
  private void load(Event event) throws EventDeliveryException {
    source.load(event);
  }

  private void commit() throws SolrServerException, IOException {
    for (SolrCollection collection : sink.getIndexer().getSolrCollections().values()) {
      ((SolrServerDocumentLoader)collection.getDocumentLoader()).getSolrServer().commit(true, true, true);
    }
  }
  
  private int queryResultSetSize(String query) throws SolrServerException, IOException {
    commit();
    int size = 0;
    for (SolrCollection collection : sink.getIndexer().getSolrCollections().values()) {
      QueryResponse rsp = ((SolrServerDocumentLoader)collection.getDocumentLoader()).getSolrServer().query(new SolrQuery(query).setRows(Integer.MAX_VALUE));
      LOGGER.debug("rsp: {}", rsp);
      size += rsp.getResults().size();
    }
    return size;
  }
  
  private static Utf8 str(String str) {
    return new Utf8(str);
  }  

  // @Test
  private void testQuery() throws Exception {
    String url = "http://127.0.0.1:8983/solr";
    HttpSolrServer server = new HttpSolrServer(url);
    server.setParser(new XMLResponseParser());
    server.deleteByQuery("*:*"); // delete everything!
    QueryResponse rsp = server.query(new SolrQuery("*:*"));
    assertEquals(0, rsp.getResults().size());
  }

}
