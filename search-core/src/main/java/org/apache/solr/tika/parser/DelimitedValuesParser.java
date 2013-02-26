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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.handler.extraction.SolrContentHandler;
import org.apache.solr.tika.ParseInfo;
import org.apache.tika.config.ServiceLoader;
import org.apache.tika.detect.AutoDetectReader;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.CloseShieldInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.sax.XHTMLContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import com.googlecode.jcsv.CSVStrategy;
import com.googlecode.jcsv.reader.CSVReader;
import com.googlecode.jcsv.reader.internal.CSVReaderBuilder;
import com.googlecode.jcsv.reader.internal.DefaultCSVEntryParser;

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
public class DelimitedValuesParser extends AbstractStreamingTikaParser {

  private char separatorChar = ',';
  private boolean ignoreFirstLine = false;
  private String[] columnNames = new String[0];
  private String columnNamesHeaderPrefix = null;
  private char quoteChar = '"';
  private char commentChar = '#';
  private boolean trim = true;

  private static final ServiceLoader LOADER = new ServiceLoader(DelimitedValuesParser.class.getClassLoader());
  private static final Logger LOGGER = LoggerFactory.getLogger(DelimitedValuesParser.class);

  public DelimitedValuesParser() {
  }

  public char getSeparatorChar() {
    return separatorChar;
  }

  public void setSeparatorChar(char separatorChar) {
    this.separatorChar = separatorChar;
  }

  public boolean isIgnoreFirstLine() {
    return ignoreFirstLine;
  }

  public void setIgnoreFirstLine(boolean ignoreFirstLine) {
    this.ignoreFirstLine = ignoreFirstLine;
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

  public char getCommentChar() {
    return commentChar;
  }

  public void setCommentChar(char commentChar) {
    this.commentChar = commentChar;
  }

  public boolean isTrim() {
    return trim;
  }

  public void setTrim(boolean trim) {
    this.trim = trim;
  }

  @Override
  /** Processes the given CSV file and writes XML into the given SAX handler */
  protected void doParse(InputStream stream, ContentHandler handler) throws IOException, SAXException, TikaException {
    ParseInfo info = getParseInfo();
    info.setMultiDocumentParser(true);
    Metadata metadata = info.getMetadata();

    // Automatically detect the character encoding
    AutoDetectReader reader = new AutoDetectReader(new CloseShieldInputStream(stream), metadata, LOADER);
    try {
      metadata.set(Metadata.CONTENT_TYPE, getSupportedTypes(info.getParseContext()).iterator().next().toString());
      XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);
      CSVReader<String[]> csvReader = createCSVReader(reader);
      List<Map.Entry<String, String>> record = new ArrayList();
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
          colNames = Arrays.copyOfRange(colValues, offset, colValues.length);
          for (int i = 0; i < colNames.length; i++) {
            colNames[i] = trim(colNames[i]);
          }
          recNum++;
          continue;
        }
        record.clear();
        for (int i = 0; i < colValues.length; i++) {
          String columnName = i < colNames.length ? colNames[i] : "col" + i; // TODO: optimize malloc
          record.add(new Entry(columnName, normalize(trim(colValues[i]))));
        }
        try {
          process(record, xhtml);
        } catch (SAXException e) {
          throw new IOException(e);
        } catch (SolrServerException e) {
          throw new IOException(e);
        }
        recNum++;
      }
      csvReader.close();
    } finally {
      reader.close();
    }
  }

  protected String normalize(String str) {
    return str;
  }

  private String trim(String str) {
    return trim ? str.trim() : str;
  }

  // TODO: consider replacing impl with http://github.com/FasterXML/jackson-dataformat-csv
  // or http://supercsv.sourceforge.net/release_notes.html
  private CSVReader createCSVReader(AutoDetectReader reader) {
    CSVStrategy strategy = new CSVStrategy(separatorChar, quoteChar, commentChar, ignoreFirstLine, true);
    return new CSVReaderBuilder(reader).strategy(strategy).entryParser(new DefaultCSVEntryParser()).build();
  }

  /** Processes the given record */
  protected void process(Iterable<Map.Entry<String, String>> record, XHTMLContentHandler handler) 
      throws IOException, SAXException, SolrServerException {
    
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("record #{}: {}", getParseInfo().getRecordNumber(), record);
    }
    List<SolrInputDocument> docs = extract(record, handler);
    docs = transform(docs);
    load(docs);
  }

  /** Extracts zero or more Solr documents from the given record */
  protected List<SolrInputDocument> extract(Iterable<Map.Entry<String, String>> record, XHTMLContentHandler handler) throws SAXException {
    SolrContentHandler solrHandler = getParseInfo().getSolrContentHandler();
    handler.startDocument();
    solrHandler.startDocument(); // this is necessary because handler.startDocument() does not delegate all the way down to solrHandler
    handler.startElement("p");
    for (Map.Entry<String, String> field : record) {
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
  protected List<SolrInputDocument> transform(List<SolrInputDocument> docs) {
    return docs;
  }

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class Entry<K,V> implements Map.Entry<K,V> {
    
    private final K key;
    private final V value;

    /**
     * Creates new entry.
     */
    public Entry(K key, V value) {
        this.value = value;
        this.key = key;
    }

    public final K getKey() {
        return key;
    }

    public final V getValue() {
        return value;
    }

    @Override
    public V setValue(V value) {
      throw new UnsupportedOperationException();
    }
  }

}
