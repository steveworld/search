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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.lang.NotImplementedException;
import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpMessage;
import org.apache.http.impl.DefaultHttpResponseFactory;
import org.apache.http.impl.conn.DefaultResponseParser;
import org.apache.http.impl.io.AbstractSessionInputBuffer;
import org.apache.http.impl.io.IdentityInputStream;
import org.apache.http.message.BasicLineParser;
import org.apache.http.mockup.SessionInputBufferMockup;
import org.apache.http.params.BasicHttpParams;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.handler.extraction.SolrContentHandler;
import org.apache.solr.tika.ParseInfo;
import org.apache.tika.exception.TikaException;
import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.extractor.ParsingEmbeddedDocumentExtractor;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.XHTMLContentHandler;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.warc.WARCConstants;
import org.archive.io.warc.WARCReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import com.google.common.io.CountingInputStream;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;

public class StreamingWarcParser extends AbstractStreamingParser {

  private Metadata metadata;

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamingWarcParser.class);
  private static final String DATE_META_KEY = "date";
  private static final String URL_META_KEY = "url";
  private static final String MIMETYPE_META_KEY = "mimetype";
  // the maximum number of bytes that will be read from any individual document in the warc
  // archive.  Any number < 0 is treated as unlimited.
  public static final String MAX_BYTES_PER_DOC_PROPERTY = "tika.warcParser.maxBytesPerDoc";
  private int maxBytesPerDoc = -1;
  // the types of mimetypes to parse.  matches are based on java.util.regex.Parser
  public static final String MIMETYPES_TO_PARSE_PROPERTY = "tika.warcParser.mimeTypes";
  private Pattern mimeTypesToParse = Pattern.compile("text/html");
  
  /**
   * Create a WarcParser
   */
  public StreamingWarcParser() {
  }

  @Override
  /** Parses the given input stream and converts it to Solr documents and loads them into Solr */
  public void parse(InputStream in, ContentHandler handler, Metadata metadata, ParseContext parseContext)
      throws IOException, SAXException, TikaException {
    
    this.metadata = metadata;
    super.parse(in, handler, metadata, parseContext);
  }

  protected void setConfigParams(Config config) {
    if (config.hasPath(MAX_BYTES_PER_DOC_PROPERTY)) {
      try {
        maxBytesPerDoc = config.getInt(MAX_BYTES_PER_DOC_PROPERTY);
      } catch (ConfigException.WrongType ce) {
        LOGGER.error("Unable to convert value for property " + MAX_BYTES_PER_DOC_PROPERTY, ce);
        throw ce;
      }
    }
    if (config.hasPath(MIMETYPES_TO_PARSE_PROPERTY)) {
      try {
        String prop = config.getString(MIMETYPES_TO_PARSE_PROPERTY);
        mimeTypesToParse = Pattern.compile(prop);
      } catch (ConfigException.WrongType ce) {
        LOGGER.error("Unable to convert value for property " + MIMETYPES_TO_PARSE_PROPERTY , ce);
        throw ce;
      } catch (PatternSyntaxException pse) {
        LOGGER.error("Unable to parse syntax for property " + MIMETYPES_TO_PARSE_PROPERTY, pse);
        throw pse;
      }
    }
  }

  @Override
  protected void doParse(InputStream in, ContentHandler handler) throws IOException, SAXException, TikaException {
    ParseInfo info = getParseInfo();
    info.setMultiDocumentParser(true);
    Config config = info.getConfig();
    setConfigParams(config);

    final ParseContext context = info.getParseContext();
    metadata.set(Metadata.CONTENT_TYPE, getSupportedTypes(context).iterator().next().toString());
    XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);
    
    String resourceName = metadata.get(metadata.RESOURCE_NAME_KEY);
    ArchiveReader ar = new UncompressedWARCReader(
      resourceName==null?"unknown warc resource":resourceName, in);

    Iterator<ArchiveRecord> it = ar.iterator();
    while (it.hasNext()) {
      ArchiveRecord record = it.next();
      ArchiveRecordHeader warcHeader = record.getHeader();
      // html files are stored in the warc as an http response.  So, we need to
      // extract the warc record, then extract the http response.

      if (warcHeader.getHeaderValue(WARCConstants.CONTENT_TYPE).equals(WARCConstants.HTTP_RESPONSE_MIMETYPE)) {
        SessionInputBufferMockup inbuffer =
          new SessionInputBufferMockup(record,
          1024, new BasicHttpParams());
        DefaultResponseParser parser = new DefaultResponseParser(
          inbuffer,
          BasicLineParser.DEFAULT,
          new DefaultHttpResponseFactory(),
          new BasicHttpParams());

        try {
          HttpMessage response = parser.parse();
          Header httpHeader = response.getLastHeader(HttpHeaders.CONTENT_TYPE);
          if (httpHeader != null) {
            MediaType mimeType = MediaType.parse(httpHeader.getValue());
            if (mimeTypesToParse.matcher(mimeType.getBaseType().toString()).matches()) {
              // convert SessionInputBuffer to an InputStream for the Tika parsers
              IdentityInputStream is = new IdentityInputStream(inbuffer);
              process(is, xhtml, warcHeader, httpHeader);
            }
          }
        } catch (HttpException ex) {
          LOGGER.warn("Unable to parse http for document: " + ex.getMessage() + " "
            + warcHeader.getRecordIdentifier() + " " + warcHeader.getUrl());
        } catch (SolrServerException e) {
          throw new IOException("Got SolrServerException while processing document: "
            + warcHeader.getRecordIdentifier() + " " + warcHeader.getUrl(), e);
        }
      }
    }
  }

  /** Processes the given Warc record */
  protected void process(InputStream inputStream, XHTMLContentHandler handler,
      ArchiveRecordHeader warcHeader, Header httpHeader)
      throws IOException, SAXException, SolrServerException, TikaException {
    List<SolrInputDocument> docs = extract(inputStream, handler, warcHeader, httpHeader);
    docs = transform(docs);
    load(docs);
  }

  protected Parser getParser(String mimeType) {
    MediaType mediaType = MediaType.parse(mimeType);
    Map<MediaType, Parser> parsers = getParseInfo().getMediaTypeToParserMap();
    Parser parser = null;

    parser = parsers.get(mediaType);
    if (parser == null && mediaType.hasParameters()) {
      parser = parsers.get(mediaType.getBaseType());
    }
    return parser;
  }

  /** Extracts zero or more Solr documents from the given Avro record */
  protected List<SolrInputDocument> extract(InputStream is, XHTMLContentHandler handler,
      ArchiveRecordHeader warcHeader, Header httpHeader)
      throws SAXException, IOException, TikaException {
    SolrContentHandler solrHandler = getParseInfo().getSolrContentHandler();
    handler.startDocument();
    solrHandler.startDocument(); // this is necessary because handler.startDocument() does not delegate all the way down to solrHandler

    Metadata entryData = new Metadata();
    entryData.set(metadata.RESOURCE_NAME_KEY, metadata.get(metadata.RESOURCE_NAME_KEY));

    Parser parser = getParser(httpHeader.getValue());
    if (parser == null) {
      throw new TikaException("Unable to find parser for mimetype " + httpHeader.getValue());
    }

    if (maxBytesPerDoc >= 0) {
      is = new BoundedInputStream(is, maxBytesPerDoc);
    }
    parser.parse(is, handler, entryData, getParseInfo().getParseContext());

    entryData.set(DATE_META_KEY, warcHeader.getDate());
    entryData.set(URL_META_KEY, warcHeader.getUrl());
    entryData.set(MIMETYPE_META_KEY, httpHeader.getValue());

    // Add the metadata added by the embedded parser into the
    // ParseInfo's metadata.  This is to guarantee that the
    // SolrContentHandler will see the correct metadata;
    // it is not guaranteed that the metadata that was passed to the
    // WarcParser is the metadata that is used by the SolrContentHandler,
    // since a parser that ran before (e.g. PackageParser) can pass in a
    // different metadata object.
    Metadata addedMetadata = new Metadata();
    Metadata parseInfoMetadata = getParseInfo().getMetadata();
    for (String name : entryData.names()) {
      // What to do if same metadata field is already written?  Let's not
      // overwrite for now.
      if (parseInfoMetadata.get(name) == null) {
        String [] values = entryData.getValues(name);
        for (String val : values) {
          parseInfoMetadata.add(name, val);
          addedMetadata.add(name, val);
        }
      }
      else {
        LOGGER.warn("Not setting metadata for: " + name +
          " because already set to: " + parseInfoMetadata.get(name));
      }
    }

    // handler.endDocument(); // this would cause a bug!
    solrHandler.endDocument();
    SolrInputDocument doc = solrHandler.newDocument().deepCopy();
    // Now that the SolrInputDocument has been generated, reset parseInfoMetdata
    // to its prior state
    for (String name : addedMetadata.names()) {
      parseInfoMetadata.remove(name);
    }

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
   * Heritrix does not currently provide an UncompressedWARCReader that takes an InputStream;
   * it assumes that the stream is compressed.  So we must create our own.
   */
  private static final class UncompressedWARCReader extends WARCReader {
    public UncompressedWARCReader(final String f, final InputStream is) {
      setIn(new CountingInputStream(is));
      initialize(f);
    }
  }
}
