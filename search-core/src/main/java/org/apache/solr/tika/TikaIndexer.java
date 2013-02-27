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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.io.StringWriter;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.handler.extraction.ExtractingParams;
import org.apache.solr.handler.extraction.ExtractingRequestHandler;
import org.apache.solr.handler.extraction.RegexRulesPasswordProvider;
import org.apache.solr.handler.extraction.SolrContentHandler;
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

import com.sun.org.apache.xml.internal.serialize.OutputFormat;
import com.sun.org.apache.xml.internal.serialize.XMLSerializer;
import com.typesafe.config.Config;

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
  private boolean useAutoGUNZIP = false;

  private static final XPathParser PARSER = new XPathParser("xhtml", XHTMLContentHandler.XHTML);

  public static final String TIKA_CONFIG_LOCATION = ExtractingRequestHandler.CONFIG_LOCATION;
  public static final String ID_PREFIX = TikaIndexer.class.getName() + ".idPrefix"; // for load testing only
  
  // pass a GZIPInputStream to tika (if detected as GZIP File).  This is temporary,
  // and thus visibility is private, until CDH-10671 is addressed.
  private static final String TIKA_AUTO_GUNZIP = "tika.autoGUNZIP";

  private static final Logger LOGGER = LoggerFactory.getLogger(TikaIndexer.class);

  public TikaIndexer(Map<String, SolrCollection> solrCollections, Config config) {
    super(solrCollections, config);
    
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
    if (config.hasPath(TIKA_AUTO_GUNZIP)) {
      useAutoGUNZIP = "true".equals(config.getString(TIKA_AUTO_GUNZIP));
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
    assert parseInfo != null;
    return parseInfo;
  }

  @Override
  protected List<SolrInputDocument> extract(StreamEvent event) {
    LOGGER.debug("event headers: {}", event.getHeaders());
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

      // Standard tika parsers occasionally have trouble with gzip data.
      // To avoid this issue, pass a GZIPInputStream if appropriate.
      InputStreamMetadata inputStreamMetadata = detectGZIPInputStream(inputStream,  metadata);
      inputStream = inputStreamMetadata.inputStream;
      // It is possible the inputStreamMetadata.metadata has a modified RESOURCE_NAME from
      // the original metadata due to how we handle GZIPInputStreams.  Pass this to tika
      // so the correct parser will be invoked (i.e. not the built-in gzip parser).
      // We leave ParseInfo.metdata untouched so it contains the correct, original resourceName.
      metadata = inputStreamMetadata.metadata;
      
      try {
        addPasswordHandler(resourceName);
        parser.parse(inputStream, parsingHandler, metadata, getParseInfo().getParseContext());
      } catch (Exception e) {
        boolean ignoreTikaException = info.getSolrCollection().getSolrParams()
            .getBool(ExtractingParams.IGNORE_TIKA_EXCEPTION, false);
        if (ignoreTikaException) {
          LOGGER.warn(new StringBuilder("Cannot parse - skipping extracting text due to ").append(e.getLocalizedMessage())
              .append(". metadata=").append(metadata.toString()).toString());
//          LOGGER.warn("Cannot parse", e);
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
  
  /**
   * @return an input stream to use, which will be a GZIPInputStream in the case
   * where the input stream is over gzipped data.
   */
  private InputStreamMetadata detectGZIPInputStream(InputStream inputStream, Metadata metadata) {
    if (useAutoGUNZIP) {
      String resourceName = metadata.get(Metadata.RESOURCE_NAME_KEY);
      if (resourceName != null && resourceName.endsWith(".gz")) {
        int magicPrefixSize = 2;
        PushbackInputStream pbis = new PushbackInputStream(inputStream, magicPrefixSize);
        try {
          byte [] readMagicPrefix = new byte[magicPrefixSize];
          int totalBytesRead = 0;
          int read;
          while (totalBytesRead != magicPrefixSize && (read = pbis.read(readMagicPrefix, totalBytesRead, magicPrefixSize - totalBytesRead )) != -1) {
            totalBytesRead += read;
          }
          if (totalBytesRead > 0) pbis.unread( readMagicPrefix, 0, totalBytesRead );
          if (totalBytesRead == magicPrefixSize) {
            // Check if stream has GZIP_MAGIC prefix
            if ((readMagicPrefix[0] == (byte) GZIPInputStream.GZIP_MAGIC)
              && (readMagicPrefix[1] == (byte) (GZIPInputStream.GZIP_MAGIC >> 8))) {
              Metadata entryData = cloneMetadata(metadata);
              // Remove the .gz extension
              String newName =
                resourceName.substring(0, resourceName.length() - ".gz".length());
              entryData.set(Metadata.RESOURCE_NAME_KEY, newName);
              return new InputStreamMetadata(new GZIPInputStream(pbis), entryData);
            }
          }
        } catch (IOException ioe) {
          LOGGER.info("Unable to read from stream to determine if gzip input stream.", ioe);
        }
        return new InputStreamMetadata(pbis, metadata);
      }
    }
    return new InputStreamMetadata(inputStream, metadata);
  }

  /**
   * @return a clone of metadata
   */
  private Metadata cloneMetadata(Metadata metadata) {
    Metadata clone = new Metadata();
    for (String name : metadata.names()) {
      String [] str = metadata.getValues(name);
      for (int i = 0; i < str.length; ++i) {
        clone.add(name, str[i]);
      }
    }
    return clone;
  }

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class InputStreamMetadata {
    
    private InputStream inputStream;
    private Metadata metadata;
 
    public InputStreamMetadata(InputStream inputStream, Metadata metadata) {
      this.inputStream = inputStream;
      this.metadata = metadata;
    }
  }
}
