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
package org.apache.flume.sink.solr;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.DateUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.handler.extraction.ExtractingParams;
import org.apache.solr.handler.extraction.ExtractingRequestHandler;
import org.apache.solr.handler.extraction.RegexRulesPasswordProvider;
import org.apache.solr.handler.extraction.SolrContentHandler;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.DefaultParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.PasswordProvider;
import org.apache.tika.sax.TeeContentHandler;
import org.apache.tika.sax.XHTMLContentHandler;
import org.apache.tika.sax.xpath.Matcher;
import org.apache.tika.sax.xpath.MatchingContentHandler;
import org.apache.tika.sax.xpath.XPathParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import com.sun.org.apache.xml.internal.serialize.OutputFormat;
import com.sun.org.apache.xml.internal.serialize.XMLSerializer;

/**
 * Flume sink that extracts search documents from Apache Flume events (using Apache Tika and Solr Cell), 
 * transforms them and loads them into Apache Solr.
 */
public class TikaSolrSink extends SimpleSolrSink implements Configurable {
    
  private int solrServerBatchSize = 1000;
  private long solrServerBatchDurationMillis = 10 * 1000;  
  private TikaConfig tikaConfig;
  private AutoDetectParser autoDetectParser;  
  private ParseContext parseContext;
    
  private static final XPathParser PARSER = new XPathParser("xhtml", XHTMLContentHandler.XHTML);

  public static final String TIKA_CONFIG_LOCATION = ExtractingRequestHandler.CONFIG_LOCATION;  
  public static final String SOLR_COLLECTION_LIST = "collection";  
  public static final String SOLR_CLIENT_HOME = "solr.home";  
  public static final String SOLR_ZK_CONNECT_STRING = "zkConnectString";
  public static final String SOLR_SERVER_URL = "url";
  public static final String SOLR_SERVER_QUEUE_LENGTH = "queueLength";
  public static final String SOLR_SERVER_NUM_THREADS = "numThreads";
  public static final String SOLR_SERVER_BATCH_SIZE = "batchSize";
  public static final String SOLR_SERVER_BATCH_DURATION_MILLIS = "batchDurationMillis";
  public static final String SOLR_HOME_PROPERTY_NAME = "solr.solr.home";

  private static final Logger LOGGER = LoggerFactory.getLogger(TikaSolrSink.class);

  public TikaSolrSink() {
  }
  
  @Override
  public void configure(Context context) {
    super.configure(context);
    
    solrServerBatchSize = context.getInteger(SOLR_SERVER_BATCH_SIZE, solrServerBatchSize);
    solrServerBatchDurationMillis = context.getLong(SOLR_SERVER_BATCH_DURATION_MILLIS, solrServerBatchDurationMillis);
    
    String tikaConfigFilePath = context.getString(TIKA_CONFIG_LOCATION);
    String oldProperty = null;
    if (tikaConfigFilePath != null) {
      oldProperty = System.setProperty("tika.config", tikaConfigFilePath); // see TikaConfig() no-arg constructor impl
    }
    
    try {
      tikaConfig = new TikaConfig();
    } catch (TikaException e) {
      throw new ConfigurationException(e);
    } catch (IOException e) {
      throw new ConfigurationException(e);
    } finally { // restore old global state
      if (tikaConfigFilePath != null) {
        if (oldProperty == null) {
          System.clearProperty("tika.config");
        } else {
          System.setProperty("tika.config", oldProperty);
        }
      }
    }
    autoDetectParser = new AutoDetectParser(tikaConfig);
  }

  protected TikaConfig getTikaConfig() {
    return tikaConfig;
  }

  @Override
  protected int getMaxBatchSize() {
    return solrServerBatchSize;
  }
  
  @Override
  protected long getMaxBatchDurationMillis() {
    return solrServerBatchDurationMillis;
  }

  /** For test injection only */
  protected List<SolrServer> createTestSolrServers() {
    return Collections.EMPTY_LIST;
  }
  
  @Override
  protected Map<String, SolrCollection> createSolrCollections() {
    Context context = getContext();
    List<SolrServer> testServers = createTestSolrServers();
    Map<String, SolrCollection> collections = new LinkedHashMap();
    Map<String, String> subContext = context.getSubProperties(SOLR_COLLECTION_LIST + ".");
    Set<String> collectionNames = new LinkedHashSet();
    for (String name : subContext.keySet()) {
      collectionNames.add(name.substring(0, name.indexOf('.')));
    }
    if (collectionNames.size() == 0) {
      throw new ConfigurationException("Missing collection specification in configuration: " + context);
    }
    
    int i = 0;
    for (String collectionName : collectionNames) {
      String solrHome = null;
      for (Map.Entry<String,String> entry : subContext.entrySet()) {
        if (entry.getKey().equals(collectionName + "." + SOLR_CLIENT_HOME)) {
          solrHome = entry.getValue();
          assert solrHome != null;
          break;
        }          
      }
      
      LOGGER.debug("solrHome: {}", solrHome);
      SolrParams params = new MapSolrParams(new HashMap());
      String zkConnectString = null;
      String solrServerUrl = "http://127.0.0.1:8983/solr/collection1";
      int solrServerNumThreads = 2;
      int solrServerQueueLength = solrServerNumThreads;
      Collection<String> dateFormats = DateUtil.DEFAULT_DATE_FORMATS;
      IndexSchema schema;
  
      String oldSolrHome = null;
      if (solrHome != null) {
        oldSolrHome = System.setProperty(SOLR_HOME_PROPERTY_NAME, solrHome);
      }
      try {        
        SolrConfig solrConfig = new SolrConfig();
        //SolrConfig solrConfig = new SolrConfig("solrconfig.xml");
        //SolrConfig solrConfig = new SolrConfig("/cloud/apache-solr-4.0.0-BETA/example/solr/collection1", "solrconfig.xml", null);
        //SolrConfig solrConfig = new SolrConfig("/cloud/apache-solr-4.0.0-BETA/example/solr/collection1/conf/solrconfig.xml");
        
        schema = new IndexSchema(solrConfig, null, null);
        //schema = new IndexSchema(solrConfig, "schema.xml", null);
        //schema = new IndexSchema(solrConfig, "/cloud/apache-solr-4.0.0-BETA/example/solr/collection1/conf/schema.xml", null);
  
        for (PluginInfo pluginInfo : solrConfig.getPluginInfos(SolrRequestHandler.class.getName())) {
          if ("/update/extract".equals(pluginInfo.name)) {
            NamedList initArgs = pluginInfo.initArgs;
            
            // Copied from StandardRequestHandler 
            if (initArgs != null) {
              Object o = initArgs.get("defaults");
              if (o != null && o instanceof NamedList) {
                SolrParams defaults = SolrParams.toSolrParams((NamedList)o);              
                params = defaults;
              }
              o = initArgs.get("appends");
              if (o != null && o instanceof NamedList) {
                SolrParams appends = SolrParams.toSolrParams((NamedList)o);
              }
              o = initArgs.get("invariants");
              if (o != null && o instanceof NamedList) {
                SolrParams invariants = SolrParams.toSolrParams((NamedList)o);
              }
              o = initArgs.get("server");
              if (o != null && o instanceof NamedList) {
                SolrParams solrServerParams = SolrParams.toSolrParams((NamedList)o);
                zkConnectString = solrServerParams.get(SOLR_ZK_CONNECT_STRING, zkConnectString);
                solrServerUrl = solrServerParams.get(SOLR_SERVER_URL, solrServerUrl);
                solrServerNumThreads = solrServerParams.getInt(SOLR_SERVER_NUM_THREADS, solrServerNumThreads);
                solrServerQueueLength = solrServerParams.getInt(SOLR_SERVER_QUEUE_LENGTH, solrServerNumThreads);
              }
              
              NamedList configDateFormats = (NamedList) initArgs.get(ExtractingRequestHandler.DATE_FORMATS);
              if (configDateFormats != null && configDateFormats.size() > 0) {
                dateFormats = new HashSet<String>();
                Iterator<Map.Entry> it = configDateFormats.iterator();
                while (it.hasNext()) {
                  String format = (String) it.next().getValue();
                  LOGGER.info("Adding Date Format: {}", format);
                  dateFormats.add(format);
                }
              }            
            }
            break; // found it
          }
        }
      } catch (ParserConfigurationException e) {
        throw new ConfigurationException(e);
      } catch (IOException e) {
        throw new ConfigurationException(e);
      } catch (SAXException e) {
        throw new ConfigurationException(e);
      } finally { // restore old global state
        if (solrHome != null) {
          if (oldSolrHome == null) {
            System.clearProperty(SOLR_HOME_PROPERTY_NAME);
          } else {
            System.setProperty(SOLR_HOME_PROPERTY_NAME, oldSolrHome);
          }
        }
      }
      
      SolrCollection solrCollection;
      if (testServers.size() > 0) {
        solrCollection = new SolrCollection(collectionName, testServers.get(i));
      } else if (zkConnectString != null) {
        try {
          solrCollection = new SolrCollection(collectionName, new CloudSolrServer(zkConnectString));
        } catch (MalformedURLException e) {
          throw new ConfigurationException(e);
        }
      } else {
        //SolrServer server = new HttpSolrServer(solrServerUrl);
        //SolrServer server = new ConcurrentUpdateSolrServer(solrServerUrl, solrServerQueueLength, solrServerNumThreads);
        SolrServer server = new SafeConcurrentUpdateSolrServer(solrServerUrl, solrServerQueueLength, solrServerNumThreads);
        solrCollection = new SolrCollection(collectionName, server);
        //server.setParser(new XMLResponseParser()); // binary parser is used by default
      }
      solrCollection.setSchema(schema);
      solrCollection.setSolrParams(params);
      solrCollection.setDateFormats(dateFormats);
      collections.put(solrCollection.getName(), solrCollection);
      i++;
    }
    return collections;
  }  

  @Override
  public void process(Event event) throws IOException, SolrServerException {
    parseContext = new ParseContext();
    parseContext.set(ParseInfo.class, new ParseInfo(event, this)); // ParseInfo is more practical than ParseContext
    try {
      super.process(event);
    } finally {
      parseContext = null;
    }    
  }
  
  protected final ParseInfo getParseInfo() {
    return parseContext.get(ParseInfo.class);
  }
  
  @Override
  protected List<SolrInputDocument> extract(Event event) {    
    Parser parser = detectParser(event);    
    parseContext.set(Parser.class, parser); // necessary for gzipped files or tar files, etc! copied from TikaCLI
    ParseInfo info = getParseInfo();
    Metadata metadata = new Metadata();
    info.setMetadata(metadata);

    // If you specify the resource name (the filename, roughly) with this parameter,
    // then Tika can make use of it in guessing the appropriate MIME type
    String resourceName = event.getHeaders().get(Metadata.RESOURCE_NAME_KEY);
    if (resourceName != null) {
      metadata.add(Metadata.RESOURCE_NAME_KEY, resourceName);
    }
    
    // Provide stream's content type as hint to Tika for better auto detection
    String contentType = event.getHeaders().get(Metadata.CONTENT_TYPE); 
    if (contentType != null) {
      metadata.add(Metadata.CONTENT_TYPE, contentType);
    }
    
    // Provide stream's charset as hint to Tika for better auto detection
    String charset = ContentStreamBase.getCharsetFromContentType(contentType);
    if (charset != null) {
      metadata.add(Metadata.CONTENT_ENCODING, charset);
    }

    info.setSolrCollection(detectSolrCollection(event, parser));
    InputStream inputStream = null;
    try {
      inputStream = TikaInputStream.get(event.getBody(), metadata);
      for (Entry<String, String> entry : event.getHeaders().entrySet()) {
        if (entry.getKey().equals(info.getSolrCollection().getSchema().getUniqueKeyField().getName())) {
          info.setId(entry.getValue()); // TODO: hack alert!
        } else {
          metadata.set(entry.getKey(), entry.getValue());
        }
      }
            
      SolrContentHandler handler = createSolrContentHandler();
      ContentHandler parsingHandler = handler;
      StringWriter debugWriter = null;
      if (LOGGER.isDebugEnabled()) {
        debugWriter = new StringWriter();
        ContentHandler serializer = new XMLSerializer(debugWriter, new OutputFormat("XML", "UTF-8", true));
        parsingHandler = new TeeContentHandler(parsingHandler, serializer); 
      }

      String xpathExpr = info.getSolrCollection().getSolrParams().get(ExtractingParams.XPATH_EXPRESSION);
//    String xpathExpr = "/xhtml:html/xhtml:body/xhtml:div/descendant:node()"; // params.get(ExtractingParams.XPATH_EXPRESSION);
      if (xpathExpr != null) {
        Matcher matcher = PARSER.parse(xpathExpr);
        parsingHandler = new MatchingContentHandler(parsingHandler, matcher);
      }

      info.setSolrContentHandler(handler);
      
      try {
        addPasswordHandler(resourceName);
        parser.parse(inputStream, parsingHandler, metadata, parseContext);
      } catch (Exception e) {
        boolean ignoreTikaException = info.getSolrCollection().getSolrParams().getBool(ExtractingParams.IGNORE_TIKA_EXCEPTION, false);
        if (ignoreTikaException) {
          LOGGER.warn(new StringBuilder("skip extracting text due to ").append(e.getLocalizedMessage())
              .append(". metadata=").append(metadata.toString()).toString());
        } else {
          throw new FlumeException(e);
        }
      }        
      
      LOGGER.debug("debug XML doc: {}", debugWriter);        
      
      if (info.isMultiDocumentParser()) {
        return Collections.EMPTY_LIST;
      }
      
      SolrInputDocument doc = handler.newDocument();
      LOGGER.debug("solr doc: {}", doc);
      return Collections.singletonList(doc);
    } finally {
      if (inputStream != null) { 
        try {
          inputStream.close();
        } catch (IOException e) {
          // ignore
        }
      }
    }
  }

  protected Parser detectParser(Event event) {
    Parser parser = autoDetectParser;
    String streamMediaType = event.getHeaders().get(ExtractingParams.STREAM_TYPE);
    if (streamMediaType != null) {
      //Cache?  Parsers are lightweight to construct and thread-safe, so I'm told
      MediaType mt = MediaType.parse(streamMediaType.trim().toLowerCase(Locale.ROOT));
      parser = new DefaultParser(getTikaConfig().getMediaTypeRegistry()).getParsers().get(mt);
      if (parser == null) {
        throw new FlumeException("Stream media type of " + streamMediaType + " didn't match any known parsers. Please supply a better " + ExtractingParams.STREAM_TYPE + " parameter.");
      }
    }
    return parser;
  }

  protected SolrCollection detectSolrCollection(Event event, Parser parser) {
    return getSolrCollections().values().iterator().next();
  }

  @Override
  public void load(List<SolrInputDocument> docs) throws IOException, SolrServerException {
    load(docs, getParseInfo().getSolrCollection().getName());
  }
  
  @Override
  public void load(List<SolrInputDocument> docs, String collectionName) throws IOException, SolrServerException {
    SolrCollection coll = getSolrCollections().get(collectionName);
    assert coll != null;
    ParseInfo info = getParseInfo();
    AtomicLong numRecords = info.getRecordNumber();
    for (SolrInputDocument doc : docs) {
      long num = numRecords.getAndIncrement();
//      LOGGER.debug("record #{} loading before doc: {}", num, doc);
      SchemaField uniqueKey = coll.getSchema().getUniqueKeyField();
      if (uniqueKey != null && !doc.containsKey(uniqueKey.getName())) {
        String id = info.getId();
        if (id == null) {
          throw new IllegalStateException("Event header " + uniqueKey.getName()
              + " must not be null as it is needed as a basis for a unique key for solr doc: " + doc);
        }
        doc.setField(uniqueKey.getName(), id + "#" + num);
      }
      LOGGER.debug("record #{} loading doc: {}", num, doc);
    }
    super.load(docs, collectionName);
  }

  protected SolrContentHandler createSolrContentHandler() {
    ParseInfo info = getParseInfo();
    SolrCollection coll = info.getSolrCollection();
    return new TrimSolrContentHandler(info.getMetadata(), coll.getSolrParams(), coll.getSchema(), coll.getDateFormats());
  }

  protected void addPasswordHandler(String resourceName) throws FileNotFoundException {
    RegexRulesPasswordProvider epp = new RegexRulesPasswordProvider();
    String pwMapFile = getParseInfo().getSolrCollection().getSolrParams().get(ExtractingParams.PASSWORD_MAP_FILE);
    if (pwMapFile != null && pwMapFile.length() > 0) {
      InputStream is = new BufferedInputStream(new FileInputStream(pwMapFile)); // getResourceLoader().openResource(pwMapFile);
      if (is != null) {
        LOGGER.debug("Password file supplied: {}", pwMapFile);
        epp.parse(is);
      }
    }
    parseContext.set(PasswordProvider.class, epp);
    String resourcePassword = getParseInfo().getSolrCollection().getSolrParams().get(ExtractingParams.RESOURCE_PASSWORD);
    if (resourcePassword != null) {
      epp.setExplicitPassword(resourcePassword);
      LOGGER.debug("Literal password supplied for file {}", resourceName);
    }
  }
    
}
