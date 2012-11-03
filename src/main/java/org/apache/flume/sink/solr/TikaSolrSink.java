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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
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
    
  protected String zkConnectString = null;
  protected String solrServerUrl = "http://127.0.0.1:8983/solr/collection1";
  protected int solrServerNumThreads = 2;
  protected int solrServerQueueLength = solrServerNumThreads;
  protected int solrServerBatchSize = 1000;
  protected long solrServerBatchDurationMillis = 10 * 1000;
  
  private TikaConfig tikaConfig;
  private IndexSchema schema;
  private SolrParams params = new MapSolrParams(new HashMap());
  private AutoDetectParser autoDetectParser;  
  private Collection<String> dateFormats = DateUtil.DEFAULT_DATE_FORMATS;
  private ParseContext ctx;
    
  private static final XPathParser PARSER = new XPathParser("xhtml", XHTMLContentHandler.XHTML);

  public static final String SOLR_CLIENT_HOME = "solr.home";  
  public static final String SOLR_ZK_CONNECT_STRING = "zkConnectString";
  public static final String SOLR_SERVER_URL = "url";
  public static final String SOLR_SERVER_QUEUE_LENGTH = "queueLength";
  public static final String SOLR_SERVER_NUM_THREADS = "numThreads";
  public static final String SOLR_SERVER_BATCH_SIZE = "batchSize";
  public static final String SOLR_SERVER_BATCH_DURATION_MILLIS = "batchDurationMillis";

  private static final Logger LOGGER = LoggerFactory.getLogger(TikaSolrSink.class);

  public TikaSolrSink() {
    this(null);
  }
  
  /** For testing only */
  protected TikaSolrSink(SolrServer server) {
    super(server);
  }
  
  @Override
  public void configure(Context context) {
    super.configure(context);
    try {
      String solrHome = context.getString(SOLR_CLIENT_HOME);
      if (solrHome != null) {
        System.setProperty("solr.solr.home", solrHome);
      }
      
      SolrConfig solrConfig = new SolrConfig();
//      SolrConfig solrConfig = new SolrConfig("solrconfig.xml");
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
              solrServerBatchSize = solrServerParams.getInt(SOLR_SERVER_BATCH_SIZE, solrServerBatchSize);
              solrServerBatchDurationMillis = solrServerParams.getInt(SOLR_SERVER_BATCH_DURATION_MILLIS, (int) solrServerBatchDurationMillis);
            }
            
            String tikaConfigLoc = (String) initArgs.get(ExtractingRequestHandler.CONFIG_LOCATION);
            if (tikaConfigLoc != null) {
              System.setProperty("tika.config", tikaConfigLoc); // see TikaConfig() no-arg constructor impl
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
    }
    
    try {
      tikaConfig = new TikaConfig();
    } catch (TikaException e) {
      throw new ConfigurationException(e);
    } catch (IOException e) {
      throw new ConfigurationException(e);
    }
    autoDetectParser = new AutoDetectParser(tikaConfig);
  }

  protected TikaConfig getTikaConfig() {
    return tikaConfig;
  }
  
  @Override
  protected SolrServer createSolrServer() {
    if (zkConnectString != null) {
      try {
        return new CloudSolrServer(zkConnectString);
      } catch (MalformedURLException e) {
        throw new ConfigurationException(e);
      }
    } else {
      //return new HttpSolrServer(solrServerUrl);
      //return new ConcurrentUpdateSolrServer(solrServerUrl, solrServerQueueLength, solrServerNumThreads);
      return new SafeConcurrentUpdateSolrServer(solrServerUrl, solrServerQueueLength, solrServerNumThreads);
      //server.setParser(new XMLResponseParser()); // binary parser is used by default
    }    
  }  

  @Override
  protected int getMaxBatchSize() {
    return solrServerBatchSize;
  }
  
  @Override
  protected long getMaxBatchDurationMillis() {
    return solrServerBatchDurationMillis;
  }

  @Override
  public void process(Event event) throws IOException, SolrServerException {
    ctx = new ParseContext(); //TODO: should we design a way to pass in parse context?
    try {
      super.process(event);
    } finally {
      ctx = null;
    }    
  }
  
  @Override
  protected List<SolrInputDocument> extract(Event event) {    
    Parser parser = detectParser(event);    
    Metadata metadata = new Metadata();

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

    InputStream inputStream = null;
    try {
      inputStream = TikaInputStream.get(event.getBody(), metadata);
      for (Entry<String, String> entry : event.getHeaders().entrySet()) {
        if (entry.getKey().equals(schema.getUniqueKeyField().getName())) {
          ctx.set(String.class, entry.getValue()); // TODO: hack alert!
        } else {
          metadata.set(entry.getKey(), entry.getValue());
        }
      }
            
      SolrContentHandler handler = createSolrContentHandler(metadata);
      ContentHandler parsingHandler = handler;
      StringWriter debugWriter = null;
      if (LOGGER.isDebugEnabled()) {
        debugWriter = new StringWriter();
        ContentHandler serializer = new XMLSerializer(debugWriter, new OutputFormat("XML", "UTF-8", true));
        parsingHandler = new TeeContentHandler(parsingHandler, serializer); 
      }

      String xpathExpr = params.get(ExtractingParams.XPATH_EXPRESSION);
//    String xpathExpr = "/xhtml:html/xhtml:body/xhtml:div/descendant:node()"; // params.get(ExtractingParams.XPATH_EXPRESSION);
//      boolean extractOnly = params.getBool(ExtractingParams.EXTRACT_ONLY, false);
      if (xpathExpr != null) {
        Matcher matcher = PARSER.parse(xpathExpr);
        parsingHandler = new MatchingContentHandler(parsingHandler, matcher);
      }

      ctx.set(Parser.class, parser); // necessary for gzipped files or tar files, etc! copied from TikaCLI
      ctx.set(TikaSolrSink.class, this);
      ctx.set(SolrContentHandler.class, handler);
      ctx.set(IndexSchema.class, schema);
      ctx.set(AtomicLong.class, new AtomicLong());
      
      try {
        addPasswordHandler(resourceName, ctx);
        parser.parse(inputStream, parsingHandler, metadata, ctx);
      } catch (Exception e) {
        boolean ignoreTikaException = params.getBool(ExtractingParams.IGNORE_TIKA_EXCEPTION, false);
        if (ignoreTikaException) {
          LOGGER.warn(new StringBuilder("skip extracting text due to ").append(e.getLocalizedMessage())
              .append(". metadata=").append(metadata.toString()).toString());
        } else {
          throw new FlumeException(e);
        }
      }        
      
      LOGGER.debug("debug XML doc: {}", debugWriter);        
      
      if (ctx.get(MultiDocumentParserMarker.class) != null) {
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

  @Override
  public void load(List<SolrInputDocument> docs) throws IOException, SolrServerException {
    AtomicLong numRecords = ctx.get(AtomicLong.class); // TODO: hack alert!
    for (SolrInputDocument doc : docs) {
      long num = numRecords.getAndIncrement();
//      LOGGER.debug("record #{} loading before doc: {}", num, doc);
      SchemaField uniqueKey = ctx.get(IndexSchema.class).getUniqueKeyField();
      if (uniqueKey != null && !doc.containsKey(uniqueKey.getName())) {
        String id = ctx.get(String.class);
        if (id == null) {
          throw new IllegalStateException("Event header " + uniqueKey.getName()
              + " must not be null as it is needed as a basis for a unique key for solr doc: " + doc);
        }
        doc.setField(uniqueKey.getName(), id + "#" + num);
      }
      LOGGER.debug("record #{} loading doc: {}", num, doc);
    }
    super.load(docs);
  }

  protected SolrContentHandler createSolrContentHandler(Metadata metadata) {
    return new TrimSolrContentHandler(metadata, params, schema, dateFormats);
  }

  protected void addPasswordHandler(String resourceName, ParseContext ctx) throws FileNotFoundException {
    RegexRulesPasswordProvider epp = new RegexRulesPasswordProvider();
    String pwMapFile = params.get(ExtractingParams.PASSWORD_MAP_FILE);
    if (pwMapFile != null && pwMapFile.length() > 0) {
      InputStream is = new BufferedInputStream(new FileInputStream(pwMapFile)); // getResourceLoader().openResource(pwMapFile);
      if (is != null) {
        LOGGER.debug("Password file supplied: {}", pwMapFile);
        epp.parse(is);
      }
    }
    ctx.set(PasswordProvider.class, epp);
    String resourcePassword = params.get(ExtractingParams.RESOURCE_PASSWORD);
    if (resourcePassword != null) {
      epp.setExplicitPassword(resourcePassword);
      LOGGER.debug("Literal password supplied for file {}", resourceName);
    }
  }

}
