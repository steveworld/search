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
package org.apache.solr.tika.parser;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.handler.extraction.SolrContentHandler;
import org.apache.solr.tika.ConfigurationException;
import org.apache.solr.tika.ParseInfo;
import org.apache.solr.tika.parser.StreamingAvroContainerParser.ForwardOnlySeekableInputStream;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.XHTMLContentHandler;
import org.apache.zookeeper.common.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import com.typesafe.config.Config;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

public class StreamingSequenceFileParser extends AbstractStreamingParser {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamingSequenceFileParser.class);
  private static final String KEY_CLASS_META_PROP = "keyClassName";
  private static final String VALUE_CLASS_META_PROP = "valueClassName";

  public static final String KEY_VALUE_PARSER_CLASS_PROP = "tika.sequenceFileParser.parserHandlerClass";
  public static final String KEY_PROP = "key";
  public static final String VALUE_PROP = "value";

  private Metadata metadata;
  private ParseContext parseContext;
  private KeyValueParserHandler keyValueParserHandler = null;

  public StreamingSequenceFileParser() {
  }

  @Override
  /** Parses the given input stream and converts it to Solr documents and loads them into Solr */
  public void parse(InputStream in, ContentHandler handler, Metadata metadata, ParseContext parseContext)
      throws IOException, SAXException, TikaException {
    this.metadata = metadata;
    this.parseContext = parseContext;
    super.parse(in, handler, metadata, parseContext);
  }
  
  private KeyValueParserHandler getKeyValueParser(
      Class<? extends KeyValueParserHandler> parserClass, Configuration conf) {
    try {
      return ReflectionUtils.newInstance(parserClass, conf);
    } catch (Exception e) {
      throw new ConfigurationException("Unexpected exception when trying to create KeyValueParserHandler of type "
        + parserClass.getName(), e);
    }
  }

  private void setConfigParams(Config config, Configuration conf) {
    if (config.hasPath(KEY_VALUE_PARSER_CLASS_PROP)) {
      String handlerStr = config.getString(KEY_VALUE_PARSER_CLASS_PROP);
      Class<? extends KeyValueParserHandler> parserClass;
      try {
        parserClass = (Class<? extends KeyValueParserHandler>)Class.forName(handlerStr);
      } catch (ClassNotFoundException cnfe) {
        throw new ConfigurationException("Could not find class "
          + handlerStr + " to use for " + KEY_VALUE_PARSER_CLASS_PROP, cnfe);
      }
      keyValueParserHandler = getKeyValueParser(parserClass, conf);
    } else {
      // use ToStringKeyvalueParserHandler as default.
      keyValueParserHandler = new ToStringKeyValueParserHandler();
    }
  }

  private void incrementCounter(SequenceFileParserCounter counter, long amount) {
    ParseInfo info = getParseInfo();
    MetricsRegistry metricsRegistry = info.getMetricsRegistry();
    Metric m = metricsRegistry.allMetrics().get(new MetricName(SequenceFileParserCounter.class, counter.toString()));
    if (m != null) {
      Counter c = (Counter)m;
      c.inc(amount);
    }
  }

  @Override
  protected void doParse(InputStream in, ContentHandler handler) throws IOException, SAXException, TikaException {
    ParseInfo info = getParseInfo();
    info.setMultiDocumentParser(true);
    Config config = info.getConfig();
    Configuration conf = new Configuration();
    setConfigParams(config, conf);

    final ParseContext context = info.getParseContext();
    metadata.set(Metadata.CONTENT_TYPE, getSupportedTypes(context).iterator().next().toString());
    XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);

    Metadata metadataToUpdate = info.getMetadata();
    FSDataInputStream fsInputStream = new FSDataInputStream(new ForwardOnlySeekable(in));
    Option opt = SequenceFile.Reader.stream(fsInputStream);
    SequenceFile.Reader reader = null;
    try {
      reader = new SequenceFile.Reader(conf, opt);
 
      metadataToUpdate.set(KEY_CLASS_META_PROP, reader.getKeyClassName());
      metadataToUpdate.set(VALUE_CLASS_META_PROP, reader.getValueClassName());
      SequenceFile.Metadata fileMetadata = reader.getMetadata();
      for (Map.Entry<Text, Text> entry : fileMetadata.getMetadata().entrySet()) {
        String key = entry.getKey().toString();
        if (metadataToUpdate.get(key) == null) {
          LOGGER.info("Setting metadata: " + key + " " + entry.getValue().toString());
          metadataToUpdate.set(key, entry.getValue().toString());
        } else {
          LOGGER.info("Not setting metadata for: " + key +
            " because already set to: " + metadataToUpdate.get(key));
        }
      }

      Writable key = (Writable)ReflectionUtils.newInstance(reader.getKeyClass(), conf);
      Writable val = (Writable)ReflectionUtils.newInstance(reader.getValueClass(), conf);
      try {
        while (reader.next(key, val)) {
          try {
            process(reader, key, val, xhtml);
          } catch (Exception ex) {
            LOGGER.warn("Exception processing key/value pair in resource: "
              + metadata.get(metadata.RESOURCE_NAME_KEY), ex);
            incrementCounter(SequenceFileParserCounter.DOC_EXCEPTIONS, 1);
          }
        }
      } catch (EOFException ex) {
        // SequenceFile.Reader will throw an EOFException after reading
        // all the data, if it doesn't know the length.  Since we are
        // passing in an InputStream, we hit this case;
        LOGGER.debug("Received expected EOFException", ex);
      } catch (Exception ex) {
        LOGGER.warn("Exception processing resource: "
          + metadata.get(metadata.RESOURCE_NAME_KEY), ex);
        incrementCounter(SequenceFileParserCounter.RESOURCE_EXCEPTIONS, 1);
      }
    } finally {
      IOUtils.closeStream(reader);
    }
  }

  /** Processes the given SequenceFile record */
  protected void process(SequenceFile.Reader reader, Object key, Object val, XHTMLContentHandler handler)
  throws IOException, SAXException, SolrServerException {
    List<SolrInputDocument> docs = extract(reader, key, val, handler);
    docs = transform(docs);
    load(docs);
  }

  /** Extracts zero or more Solr documents from the given SequenceFile record */
  protected List<SolrInputDocument> extract(SequenceFile.Reader reader, Object key, Object val, XHTMLContentHandler handler)
  throws SAXException, IOException {
    SolrContentHandler solrHandler = getParseInfo().getSolrContentHandler();
    handler.startDocument();
    solrHandler.startDocument(); // this is necessary because handler.startDocument() does not delegate all the way down to solrHandler
    handler.startElement("p");
    parseSeqFileKeyValue(reader, key, val, handler);
    handler.endElement("p");
    // handler.endDocument(); // this would cause a bug!
    solrHandler.endDocument();
    SolrInputDocument doc = solrHandler.newDocument().deepCopy();
    return Collections.singletonList(doc);
  }

  /**
   * Extension point to transform a list of documents in an application specific
   * way. Does nothing by default
   */
  protected List<SolrInputDocument> transform(List<SolrInputDocument> docs) {
    return docs;
  }

  /**
   * Calls the supplied keyValueParserHandler to parse the current key/value pair.
   */
  protected void parseSeqFileKeyValue(SequenceFile.Reader reader, Object key, Object val, XHTMLContentHandler handler)
  throws IOException {
    keyValueParserHandler.parseSeqFileKeyValue(reader, key, val, handler, this.metadata, this.parseContext);
  }

  /**
   * Interface for implementing arbitrary parsing for key/values in the SequenceFile.
   * Specify by setting property SequenceFileParser.KEY_VALUE_PARSER_PROP
   */
  public static interface KeyValueParserHandler {
    /**
     * Parse the given key/value using the parse parameters
     */
    void parseSeqFileKeyValue(SequenceFile.Reader reader, Object key, Object value,
        XHTMLContentHandler handler, Metadata metadata, ParseContext parseContext) throws IOException;
  }

  /**
   * Default KeyValueParserHandler that uses toString()
   */
  private static class ToStringKeyValueParserHandler implements KeyValueParserHandler {
    /**
     * Parse the given key/value using the parse parameters
     */
    public void parseSeqFileKeyValue(SequenceFile.Reader reader, Object key, Object value,
        XHTMLContentHandler handler, Metadata metadata, ParseContext parseContext) throws IOException{
      try {
        handler.startElement("key");
        handler.characters(key.toString());
        handler.endElement("key");
        handler.startElement("value");
        handler.characters(value.toString());
        handler.endElement("value");
      } catch (SAXException saex) {
        throw new IOException(saex);
      }
    }
  }

  /**
   * Forward-only Seekable InputStream to use for reading SequenceFiles.  Will throw an exception if
   * an attempt is made to seek backwards.
   */
  private static class ForwardOnlySeekable extends InputStream implements Seekable, PositionedReadable {
    private ForwardOnlySeekableInputStream fosInputStream;

    public ForwardOnlySeekable(InputStream inputStream) {
      this.fosInputStream = new ForwardOnlySeekableInputStream(inputStream);
    }

    /**
     * Seek to the given offset from the start of the file.
     * The next read() will be from that location.  Can't
     * seek past the end of the file.
     */
    public void seek(long pos) throws IOException {
      fosInputStream.seek(pos);
    }
    
    /**
     * Return the current offset from the start of the file
     */
    public long getPos() throws IOException {
      return fosInputStream.tell();
    }

    /**
     * Seeks a different copy of the data.  Returns true if
     * found a new source, false otherwise.
     */
    public boolean seekToNewSource(long targetPos) throws IOException {
      throw new NotImplementedException("not implemented!");
    }

    /**
     * Read upto the specified number of bytes, from a given
     * position within a file, and return the number of bytes read. This does not
     * change the current offset of a file, and is thread-safe.
     */
    public int read(long position, byte[] buffer, int offset, int length)
    throws IOException {
      throw new NotImplementedException("not implemented!");
    }

    /**
     * Read the specified number of bytes, from a given
     * position within a file. This does not
     * change the current offset of a file, and is thread-safe.
     */
    public void readFully(long position, byte[] buffer, int offset, int length)
    throws IOException {
      throw new NotImplementedException("not implemented!");
    }

    /**
     * Read number of bytes equal to the length of the buffer, from a given
     * position within a file. This does not
     * change the current offset of a file, and is thread-safe.
     */
    public void readFully(long position, byte[] buffer) throws IOException {
      throw new NotImplementedException("not implemented!");
    }

    public int read() throws IOException {
      byte [] b = new byte[1];
      int len = fosInputStream.read(b, 0, 1);
      int ret = (len == -1)? -1 : b[0] & 0xFF;
      return ret;
    }
  }
}
