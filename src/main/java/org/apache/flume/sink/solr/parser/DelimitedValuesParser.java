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
package org.apache.flume.sink.solr.parser;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flume.sink.solr.ParseInfo;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.handler.extraction.SolrContentHandler;
import org.apache.tika.config.ServiceLoader;
import org.apache.tika.detect.AutoDetectReader;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.CloseShieldInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.XHTMLContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import au.com.bytecode.opencsv.CSVReader;

/**
 * Comma separated values parser that extracts search documents from CSV records
 * (using Apache Tika and Solr Cell) and loads them into Solr.
 * 
 * For the format see http://en.wikipedia.org/wiki/Comma-separated_values and
 * http://www.creativyst.com/Doc/Articles/CSV/CSV01.htm and
 * http://ostermiller.org/utils/CSV.html and
 * http://www.ricebridge.com/products/csvman/demo.htm
 * 
 * The text encoding of the document stream is automatically detected based on
 * the byte patterns found at the beginning of the stream and the given document
 * metadata, most notably the <code>charset</code> parameter of a
 * {@link Metadata#CONTENT_TYPE} value.
 * <p>
 * This parser sets the following output metadata entries:
 * <dl>
 * <dt>{@link Metadata#CONTENT_TYPE}</dt>
 * <dd><code>text/csv; charset=...</code></dd>
 * </dl>
 */
public class DelimitedValuesParser extends AbstractParser {

  private char separatorChar = ',';
  private int numLeadingLinesToSkip = 0;
  private String[] columnNames = new String[0];
  private String columnNamesHeaderPrefix = null;
  private char quoteChar = au.com.bytecode.opencsv.CSVParser.DEFAULT_QUOTE_CHARACTER;
  private String commentChars = "!#;";
  private boolean trim = true;
  private Set<MediaType> supportedMediaTypes;

  private static final ServiceLoader LOADER = new ServiceLoader(DelimitedValuesParser.class.getClassLoader());
  private static final Logger LOGGER = LoggerFactory.getLogger(DelimitedValuesParser.class);
  private static final long serialVersionUID = -6656103329236888910L;

  public DelimitedValuesParser() {
  }

  public char getSeparatorChar() {
    return separatorChar;
  }

  public void setSeparatorChar(char separatorChar) {
    this.separatorChar = separatorChar;
  }

  public int getNumLeadingLinesToSkip() {
    return numLeadingLinesToSkip;
  }

  public void setNumLeadingLinesToSkip(int numLeadingLinesToSkip) {
    this.numLeadingLinesToSkip = numLeadingLinesToSkip;
  }

  public String[] getColumnNames() {
    return columnNames;
  }

  public void setColumnNames(String[] columnNames) {
    this.columnNames = columnNames;
  }

  public String getColumnNamesHeaderPrefix() {
    return columnNamesHeaderPrefix;
  }

  public void setColumnNamesHeaderPrefix(String columnNamesHeaderPrefix) {
    this.columnNamesHeaderPrefix = columnNamesHeaderPrefix;
  }

  public String getCommentChars() {
    return commentChars;
  }

  public void setCommentChars(String commentChars) {
    this.commentChars = commentChars;
  }

  public boolean isTrim() {
    return trim;
  }

  public void setTrim(boolean trim) {
    this.trim = trim;
  }

  @Override
  public Set<MediaType> getSupportedTypes(ParseContext context) {
    return supportedMediaTypes;
  }

  public void setSupportedTypes(Set<MediaType> supportedMediaTypes) {
    this.supportedMediaTypes = supportedMediaTypes;
  }

  @Override
  /** Processes the given CSV file and writes XML into the given SAX handler */
  public void parse(InputStream in, ContentHandler handler, Metadata metadata, ParseContext context)
      throws IOException, SAXException, TikaException {
    try {
      parse2(in, handler, metadata, context);
    } catch (Exception e) {
      LOGGER.error("Cannot parse", e);
      throw new IOException(e);
    }
  }

  protected void parse2(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext context)
      throws IOException, TikaException {

    getParseInfo(context).setMultiDocumentParser(true); // TODO hack alert!

    // Automatically detect the character encoding
    AutoDetectReader reader = new AutoDetectReader(new CloseShieldInputStream(stream), metadata, LOADER);
    try {
      metadata.set(Metadata.CONTENT_TYPE, supportedMediaTypes.iterator().next().toString());
      XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);

      CSVReader csvReader = createCSVReader(reader, metadata, context);
      Map<String, String> record = new LinkedHashMap();
      String[] colNames = columnNames;
      String[] colValues;
      long recNum = 0;
      while ((colValues = csvReader.readNext()) != null) {
        if (recNum == 0 && columnNamesHeaderPrefix != null && colValues.length > 0
            && colValues[0].startsWith(columnNamesHeaderPrefix)) {
          int offset = 0; // it is a header line
          if (colValues[0].equals(columnNamesHeaderPrefix)) {
            offset = 1; // exclude prefix field from the names
          } else {
            // exclude prefix substring from name
            colValues[0] = colValues[0].substring(columnNamesHeaderPrefix.length()); 
          }
          colNames = new String[colValues.length - offset];
          System.arraycopy(colValues, offset, colNames, 0, colNames.length);
          for (int i = 0; i < colNames.length; i++) {
            colNames[i] = trim(colNames[i]);
          }
          recNum++;
          continue;
        }
        if (colValues.length > 0 && colValues[0].length() > 0 && commentChars.indexOf(colValues[0].charAt(0)) >= 0) {
          continue; // it is a comment line
        }
        record.clear();
        for (int i = 0; i < colValues.length; i++) {
          String columnName = i < colNames.length ? colNames[i] : "col" + i;
          record.put(columnName, normalize(trim(colValues[i])));
        }
        process(record, xhtml, metadata, context);
        recNum++;
      }
      csvReader.close();
    } catch (Exception e) {
      e.printStackTrace();
      LOGGER.error("Cannot parse", e);
      throw new IOException(e);
    } finally {
      reader.close();
    }
  }

  protected ParseInfo getParseInfo(ParseContext context) {
    return context.get(ParseInfo.class);
  }

  protected String normalize(String str) {
    return str;
  }

  private String trim(String str) {
    return trim ? str.trim() : str;
  }

  private CSVReader createCSVReader(AutoDetectReader reader, Metadata metadata, ParseContext context) {
    return new CSVReader(reader, separatorChar, quoteChar, numLeadingLinesToSkip);
  }

  /** Processes the given record */
  protected void process(Map<String, String> record, XHTMLContentHandler handler, Metadata metadata,
      ParseContext context) throws IOException, SAXException, SolrServerException {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("record #{}: {}", getParseInfo(context).getRecordNumber(), record);
    }
    List<SolrInputDocument> docs = extract(record, handler, metadata, context);
    docs = transform(docs, metadata, context);
    load(docs, metadata, context);
  }

  /** Extracts zero or more Solr documents from the given record */
  protected List<SolrInputDocument> extract(Map<String, String> record, XHTMLContentHandler handler, Metadata metadata,
      ParseContext context) throws SAXException {
    SolrContentHandler solrHandler = getParseInfo(context).getSolrContentHandler();
    handler.startDocument();
    solrHandler.startDocument(); // this is necessary because handler.startDocument() does not delegate all the way down to solrHandler
    handler.startElement("p");
    for (Map.Entry<String, String> field : record.entrySet()) {
      handler.element(field.getKey(), field.getValue());
    }
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
  protected List<SolrInputDocument> transform(List<SolrInputDocument> docs, Metadata metadata, ParseContext context) {
    return docs;
  }

  /** Loads the given documents into Solr */
  protected void load(List<SolrInputDocument> docs, Metadata metadata, ParseContext context) throws IOException,
      SolrServerException {
    getParseInfo(context).getSink().load(docs);
  }

}
