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
package org.apache.flume.sink.solr.indexer.parser;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;
import org.apache.flume.sink.solr.indexer.ParseInfo;
import org.apache.http.mockup.SessionInputBufferMockup;
import org.apache.http.impl.DefaultHttpResponseFactory;
import org.apache.http.impl.conn.DefaultResponseParser;
import org.apache.http.impl.io.AbstractSessionInputBuffer;
import org.apache.http.message.BasicLineParser;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpMessage;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.handler.extraction.SolrContentHandler;
import org.apache.tika.exception.TikaException;
import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.extractor.ParsingEmbeddedDocumentExtractor;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
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

/**
 * Warc parser that extracts search documents from warc files (using Apache
 * Tika and Solr Cell) and loads them into Solr.
 *
 * Always emits the following fields:
 * id:            unique identifier for document
 * text:          text of the document (i.e. body of html)
 * date:          date the document was retrieved
 * url:           url on which the document was retrieved
 *
 * Emits the following fields if the document contains them:
 * mimetype:      e.g. "text/html; charset=UTF-8"
 * copyright
 * title
 * description
 * keywords
 * language       e.g. "en-us"
 */
public class WarcParser extends AbstractParser {

  private Set<MediaType> supportedMediaTypes;

  private static final Logger LOGGER = LoggerFactory.getLogger(WarcParser.class);
  private static final long serialVersionUID = -6656103329236898912L;
  private EmbeddedDocumentExtractor extractor;
  private static String DATE_META_KEY = "date";
  private static String URL_META_KEY = "url";
  private static String MIMETYPE_META_KEY = "mimetype";

  /**
   * Create a WarcParser
   */
  public WarcParser() {
    setSupportedTypes(Collections.singleton(MediaType.parse("warc/text")));
  }

  @Override
  /** {@inheritDoc} */
  public Set<MediaType> getSupportedTypes(ParseContext context) {
    return supportedMediaTypes;
  }

  protected void setSupportedTypes(Set<MediaType> supportedMediaTypes) {
    this.supportedMediaTypes = supportedMediaTypes;
  }

  @Override
  /** Processes the given Warc file and converts records to solr documents and loads them into Solr
      {@inheritDoc}
   */
  public void parse(InputStream in, ContentHandler handler, Metadata metadata, ParseContext context)
      throws IOException, SAXException, TikaException {
    try {
      parse2(in, handler, metadata, context);
    } finally {
      in.close();
    }
  }

  /**
   * Heritrix does not currently provide an UncompressedWARCReader that takes an InputStream;
   * it assumes that the stream is compressed.  So we must create our own.
   */
  class UncompressedWARCReader extends WARCReader {
    public UncompressedWARCReader(final String f, final InputStream is) {
      setIn(new CountingInputStream(is));
      initialize(f);
    }
  }

  /**
   * InputStream implementation with an underlying AbstractSessionInputBuffer.
   * This is necessary to avoid doing an extra copy; the DefaultHttpResponseParser
   * uses a SessionInputBuffer (not an InputStream), but the next-level parser
   * needs an InputStream.
   * FixMe: is this actually necessary?  We don't currently pass an InputStream
   * directly into a next-level parser.
   */
  class SessionInputStream extends InputStream {
    private AbstractSessionInputBuffer sessionInputBuffer;
    public SessionInputStream(AbstractSessionInputBuffer sessionInputBuffer) {
      this.sessionInputBuffer = sessionInputBuffer;
    }
    public int available() { return sessionInputBuffer.available(); }
    public void close() {} // do nothing
    public void mark(int readlimit) {} // do nothing
    public boolean markSupported() { return false; }
    public int read() throws IOException { return sessionInputBuffer.read(); }
    public int read(byte[] b) throws IOException { return sessionInputBuffer.read(b);}
    public int read(byte[] b, int off, int len) throws IOException {
      return sessionInputBuffer.read(b, off, len);
    }
    public void reset() {} // do nothing
    public long skip(long n) { throw new NotImplementedException(); }
  }

  protected void parse2(InputStream in, ContentHandler handler, Metadata metadata, ParseContext context)
      throws IOException, SAXException {
    getParseInfo(context).setMultiDocumentParser(true); // TODO hack alert!

    metadata.set(Metadata.CONTENT_TYPE, getSupportedTypes(context).iterator().next().toString());
    XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);
    // Use the delegate parser to parse the contained document
    extractor = context.get(
      EmbeddedDocumentExtractor.class, new ParsingEmbeddedDocumentExtractor(context));

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
        byte [] warcDoc = new byte[(int)(warcHeader.getLength())];
        int length = record.read(warcDoc);
        SessionInputBufferMockup inbuffer =
          new SessionInputBufferMockup(new ByteArrayInputStream(warcDoc),
          length, new BasicHttpParams());
        DefaultResponseParser parser = new DefaultResponseParser(
          inbuffer,
          BasicLineParser.DEFAULT,
          new DefaultHttpResponseFactory(),
          new BasicHttpParams());

        try {
          HttpMessage response = parser.parse();
          Header httpHeader = response.getLastHeader(HttpHeaders.CONTENT_TYPE);
          metadata.set(DATE_META_KEY, warcHeader.getDate());
          metadata.set(URL_META_KEY, warcHeader.getUrl());
          metadata.set(MIMETYPE_META_KEY, httpHeader.getValue());
          if (httpHeader != null && httpHeader.getValue().startsWith(MediaType.TEXT_HTML.toString())) {
            SessionInputStream sessionInputStream = new SessionInputStream(inbuffer);
            process(sessionInputStream, xhtml, metadata, context);
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

  protected ParseInfo getParseInfo(ParseContext context) {
    return context.get(ParseInfo.class);
  }

  /** Processes the given Warc record */
  protected void process(SessionInputStream sessionInputStream, XHTMLContentHandler handler,
      Metadata metadata, ParseContext context)
      throws IOException, SAXException, SolrServerException {
    List<SolrInputDocument> docs = extract(sessionInputStream, handler, metadata, context);
    docs = transform(docs, metadata, context);
    load(docs, metadata, context);
  }

  /** Extracts zero or more Solr documents from the given Avro record */
  protected List<SolrInputDocument> extract(SessionInputStream is, XHTMLContentHandler handler,
      Metadata metadata, ParseContext context) throws SAXException, IOException {
    SolrContentHandler solrHandler = getParseInfo(context).getSolrContentHandler();
    handler.startDocument();
    solrHandler.startDocument(); // this is necessary because handler.startDocument() does not delegate all the way down to solrHandler

    if (extractor.shouldParseEmbedded(metadata)) {
      extractor.parseEmbedded(is, handler, metadata, true);
    }

    // Add the metadata added by the embedded parser into the
    // ParseInfo's metadata.  This is to guarantee that the
    // SolrContentHandler will see the correct metadata;
    // it is not guaranteed that the metadata that was passed to the
    // WarcParser is the metadata that is used by the SolrContentHandler,
    // since a parser that ran before (e.g. PackageParser) can pass in a
    // different metadata object.
    Metadata parseInfoMetadata = getParseInfo(context).getMetadata();
    for (String name : metadata.names()) {
      // What to do if same metadata field is already written?  Let's not
      // overwrite for now.
      if (parseInfoMetadata.get(name) == null) {
        String [] values = metadata.getValues(name);
        for (String val : values) {
          parseInfoMetadata.add(name, val);
        }
      }
      else {
        LOGGER.warn("Not setting metadata for: " + name +
          " because already set to: " + metadata.get(name));
      }
    }
    // handler.endDocument(); // this would cause a bug!
    solrHandler.endDocument();
    SolrInputDocument doc = solrHandler.newDocument().deepCopy();
    return Collections.singletonList(doc);
  }

  /**
   * Extension point to transform a list of documents in an application specific
   * way. Does nothing by default
   */
  protected List<SolrInputDocument> transform(List<SolrInputDocument> docs, Metadata metadata, ParseContext context) {
    return docs;
  }

  /** Loads the given documents into Solr */
  protected void load(List<SolrInputDocument> docs, Metadata metadata, ParseContext context)
  throws IOException, SolrServerException {
    getParseInfo(context).getIndexer().load(docs);
  }

}
