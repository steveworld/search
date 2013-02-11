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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.security.SecureRandom;
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
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import javax.xml.parsers.ParserConfigurationException;

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
import org.apache.tika.parser.CompositeParser;
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
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

/**
 * Indexer that extracts search documents from events (using Apache Tika and
 * Solr Cell), transforms them and loads them into Apache Solr.
 */
public class TikaIndexer extends SolrIndexer {

  private TikaConfig tikaConfig;
  private AutoDetectParser autoDetectParser;
  private ParseInfo parseInfo;

  private String idPrefix; // for load testing only; enables adding same document many times with a different unique key
  private Random randomIdPrefix; // for load testing only; enables adding same document many times with a different unique key

  private static final XPathParser PARSER = new XPathParser("xhtml", XHTMLContentHandler.XHTML);

  public static final String TIKA_CONFIG_LOCATION = ExtractingRequestHandler.CONFIG_LOCATION;
  public static final String SOLR_COLLECTION_LIST = "collection";
  public static final String SOLR_CLIENT_HOME = "solr.home";
  public static final String SOLR_ZK_CONNECT_STRING = "zkConnectString";
  public static final String SOLR_SERVER_URL = "url";
  public static final String SOLR_SERVER_QUEUE_LENGTH = "queueLength";
  public static final String SOLR_SERVER_NUM_THREADS = "numThreads";
  public static final String SOLR_HOME_PROPERTY_NAME = "solr.solr.home";
  public static final String ID_PREFIX = "TikaIndexer.idPrefix"; // for load testing only

  private static final Logger LOGGER = LoggerFactory.getLogger(TikaIndexer.class);

  public TikaIndexer() {
  }

  @Override
  public void configure(Config config) {
    super.configure(config);

    String tikaConfigFilePath = null;
    if (config.hasPath(TIKA_CONFIG_LOCATION)) {
      tikaConfigFilePath = config.getString(TIKA_CONFIG_LOCATION);
      File file = new File(tikaConfigFilePath); 
      if (!file.exists()) {
        throw new ConfigurationException("File not found: " + file + " absolutePath: " + file.getAbsolutePath());
      }
    }
    String oldProperty = null;
    if (tikaConfigFilePath != null) {
      // see TikaConfig() no-arg constructor impl
      oldProperty = System.setProperty("tika.config", tikaConfigFilePath);
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
        
    if (config.hasPath(ID_PREFIX)) { // for load testing only
      idPrefix = config.getString(ID_PREFIX);
    }
    if ("random".equals(idPrefix)) { // for load testing only
      randomIdPrefix = new Random(new SecureRandom().nextLong());    
      idPrefix = null;
    }
  }
  
  protected TikaConfig getTikaConfig() {
    return tikaConfig;
  }

  @Override
  public void process(StreamEvent event) throws IOException, SolrServerException {
    parseInfo = new ParseInfo(event, this); // ParseInfo is more practical than ParseContext
    try {
      super.process(event);
    } finally {
      parseInfo = null;
    }
  }

  protected final ParseInfo getParseInfo() {
    return parseInfo;
  }

  @Override
  protected List<SolrInputDocument> extract(StreamEvent event) {
    Parser parser = detectParser(event);
    ParseInfo info = getParseInfo();

    // necessary for gzipped files or tar files, etc! copied from TikaCLI
    info.getParseContext().set(Parser.class, parser);

    Metadata metadata = info.getMetadata();

    // If you specify the resource name (the filename, roughly) with this
    // parameter, then Tika can use it in guessing the right MIME type
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
      inputStream = TikaInputStream.get(event.getBody());
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
      if (LOGGER.isTraceEnabled()) {
        debugWriter = new StringWriter();
        ContentHandler serializer = new XMLSerializer(debugWriter, new OutputFormat("XML", "UTF-8", true));
        parsingHandler = new TeeContentHandler(parsingHandler, serializer);
      }

      String xpathExpr = info.getSolrCollection().getSolrParams().get(ExtractingParams.XPATH_EXPRESSION);
      // String xpathExpr =
      // "/xhtml:html/xhtml:body/xhtml:div/descendant:node()";
      if (xpathExpr != null) {
        Matcher matcher = PARSER.parse(xpathExpr);
        parsingHandler = new MatchingContentHandler(parsingHandler, matcher);
      }

      info.setSolrContentHandler(handler);

      try {
        addPasswordHandler(resourceName);
        parser.parse(inputStream, parsingHandler, metadata, getParseInfo().getParseContext());
      } catch (Exception e) {
        boolean ignoreTikaException = info.getSolrCollection().getSolrParams()
            .getBool(ExtractingParams.IGNORE_TIKA_EXCEPTION, false);
        if (ignoreTikaException) {
          LOGGER.warn(new StringBuilder("skip extracting text due to ").append(e.getLocalizedMessage())
              .append(". metadata=").append(metadata.toString()).toString());
        } else {
          throw new IndexerException(e);
        }
      }

      LOGGER.trace("debug XML doc: {}", debugWriter);

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

  protected Parser detectParser(StreamEvent event) {
    Parser parser = autoDetectParser;
    String streamMediaType = event.getHeaders().get(ExtractingParams.STREAM_TYPE);
    if (streamMediaType != null) {
      // Cache? Parsers are lightweight to construct and thread-safe, so I'm told
      MediaType mt = MediaType.parse(streamMediaType.trim().toLowerCase(Locale.ROOT));
      CompositeParser tikaConfigParser = (CompositeParser) getTikaConfig().getParser();
//      DefaultParser tikaConfigParser = new DefaultParser(getTikaConfig().getMediaTypeRegistry());
      parser = tikaConfigParser.getParsers().get(mt);
      if (parser == null) {
        throw new IndexerException("Stream media type of " + streamMediaType
            + " didn't match any known parsers. Please supply a better " + ExtractingParams.STREAM_TYPE + " parameter.");
      }
    }
    return parser;
  }

  protected SolrCollection detectSolrCollection(StreamEvent event, Parser parser) {
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
    AtomicLong numRecords = getParseInfo().getRecordNumber();
    for (SolrInputDocument doc : docs) {
      long num = numRecords.getAndIncrement();
      // LOGGER.debug("record #{} loading before doc: {}", num, doc);
      SchemaField uniqueKey = coll.getSchema().getUniqueKeyField();
      if (uniqueKey != null && !doc.containsKey(uniqueKey.getName())) {
        String id = getParseInfo().getId();
        if (id == null) {
          throw new IllegalStateException("Event header " + uniqueKey.getName()
              + " must not be null as it is needed as a basis for a unique key for solr doc: " + doc);
        }
        doc.setField(uniqueKey.getName(), id + "#" + num);
      }
      
      // for load testing only; enables adding same document many times with a different unique key
      if (idPrefix != null) { 
        String id = doc.getFieldValue(uniqueKey.getName()).toString();
        id = idPrefix + id;
        doc.setField(uniqueKey.getName(), id);
      } else if (randomIdPrefix != null) {
        String id = doc.getFieldValue(uniqueKey.getName()).toString();
        id = String.valueOf(Math.abs(randomIdPrefix.nextInt())) + "#" + id;
        doc.setField(uniqueKey.getName(), id);
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

  /** For test injection only */
  protected List<DocumentLoader> createTestSolrServers() {
    return Collections.EMPTY_LIST;
  }

  @Override
  protected Map<String, SolrCollection> createSolrCollections() {
    /*
     * TODO: need to add an API to solrj that allows fetching IndexSchema from
     * the remote Solr server. Plus move Solr cell params out of solrconfig.xml
     * into a nice HOCON config file. This would allow us to have a single
     * source of truth, simplify and make it unnecessary to parse schema.xml and
     * solrconfig.xml on the client side.
     */
    Config context = getConfig().getTreeConfig();
    List<DocumentLoader> testServers = createTestSolrServers();
    Map<String, SolrCollection> collections = new LinkedHashMap();
    Config subContext = context.getConfig(SOLR_COLLECTION_LIST);
    Set<String> collectionNames = new LinkedHashSet();
    for (Entry<String, ConfigValue> entry : subContext.entrySet()) {
      String name = entry.getKey();
      collectionNames.add(name.substring(0, name.indexOf('.')));
    }
    if (collectionNames.size() == 0) {
      throw new ConfigurationException("Missing collection specification in configuration: " + context);
    }

    int i = 0;
    for (String collectionName : collectionNames) {
      String solrHome = null;
      for (Map.Entry<String, ConfigValue> entry : subContext.entrySet()) {
        if (entry.getKey().equals(collectionName + "." + SOLR_CLIENT_HOME)) {
          solrHome = entry.getValue().unwrapped().toString();
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
        // SolrConfig solrConfig = new SolrConfig("solrconfig.xml");
        // SolrConfig solrConfig = new
        // SolrConfig("/cloud/apache-solr-4.0.0-BETA/example/solr/collection1",
        // "solrconfig.xml", null);
        // SolrConfig solrConfig = new
        // SolrConfig("/cloud/apache-solr-4.0.0-BETA/example/solr/collection1/conf/solrconfig.xml");

        schema = new IndexSchema(solrConfig, null, null);
        // schema = new IndexSchema(solrConfig, "schema.xml", null);
        // schema = new IndexSchema(solrConfig,
        // "/cloud/apache-solr-4.0.0-BETA/example/solr/collection1/conf/schema.xml",
        // null);

        for (PluginInfo pluginInfo : solrConfig.getPluginInfos(SolrRequestHandler.class.getName())) {
          if ("/update/extract".equals(pluginInfo.name)) {
            NamedList initArgs = pluginInfo.initArgs;

            // Copied from StandardRequestHandler
            if (initArgs != null) {
              Object o = initArgs.get("defaults");
              if (o != null && o instanceof NamedList) {
                SolrParams defaults = SolrParams.toSolrParams((NamedList) o);
                params = defaults;
              }
              o = initArgs.get("appends");
              if (o != null && o instanceof NamedList) {
                SolrParams appends = SolrParams.toSolrParams((NamedList) o);
              }
              o = initArgs.get("invariants");
              if (o != null && o instanceof NamedList) {
                SolrParams invariants = SolrParams.toSolrParams((NamedList) o);
              }
              o = initArgs.get("server");
              if (o != null && o instanceof NamedList) {
                SolrParams solrServerParams = SolrParams.toSolrParams((NamedList) o);
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
          solrCollection = new SolrCollection(collectionName, new SolrServerDocumentLoader(new CloudSolrServer(
              zkConnectString)));
        } catch (MalformedURLException e) {
          throw new ConfigurationException(e);
        }
      } else {
        // SolrServer server = new HttpSolrServer(solrServerUrl);
        // SolrServer server = new ConcurrentUpdateSolrServer(solrServerUrl,
        // solrServerQueueLength, solrServerNumThreads);
        SolrServer server = new SafeConcurrentUpdateSolrServer(solrServerUrl, solrServerQueueLength,
            solrServerNumThreads);
        solrCollection = new SolrCollection(collectionName, new SolrServerDocumentLoader(server));
        // server.setParser(new XMLResponseParser()); // binary parser is used
        // by default
      }
      solrCollection.setSchema(schema);
      solrCollection.setSolrParams(params);
      solrCollection.setDateFormats(dateFormats);
      collections.put(solrCollection.getName(), solrCollection);
      i++;
    }
    return collections;
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
    getParseInfo().getParseContext().set(PasswordProvider.class, epp);
    String resourcePassword = getParseInfo().getSolrCollection().getSolrParams()
        .get(ExtractingParams.RESOURCE_PASSWORD);
    if (resourcePassword != null) {
      epp.setExplicitPassword(resourcePassword);
      LOGGER.debug("Literal password supplied for file {}", resourceName);
    }
  }

}
