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

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

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
 * Comma separated values parser that extracts search documents from CSV records (using Apache Tika and Solr Cell) and
 * loads them into Solr.
 * 
 * The text encoding of the document stream is
 * automatically detected based on the byte patterns found at the
 * beginning of the stream and the given document metadata, most
 * notably the <code>charset</code> parameter of a
 * {@link Metadata#CONTENT_TYPE} value.
 * <p>
 * This parser sets the following output metadata entries:
 * <dl>
 *   <dt>{@link Metadata#CONTENT_TYPE}</dt>
 *   <dd><code>text/csv; charset=...</code></dd>
 * </dl>
 */
public class CSVParser extends AbstractParser {

  // TODO: make column names and separator char and comment chars, etc configurable

  protected AtomicLong numRecords = new AtomicLong();

  private static final MediaType MEDIATYPE_CSV = MediaType.parse("text/csv");
  private static final Set<MediaType> SUPPORTED_TYPES = Collections.singleton(MEDIATYPE_CSV);
  private static final ServiceLoader LOADER = new ServiceLoader(CSVParser.class.getClassLoader());
  private static final Logger LOGGER = LoggerFactory.getLogger(CSVParser.class);
  private static final long serialVersionUID = -6656103329236888910L;

  @Override
  public Set<MediaType> getSupportedTypes(ParseContext context) {
    return SUPPORTED_TYPES;
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
    //if (true) throw new RuntimeException("reached csvparser");
 
    context.set(MultiDocumentParserMarker.class, new MultiDocumentParserMarker()); // TODO hack alert!    
    numRecords = context.get(AtomicLong.class); // TODO hack alert!
    
    // Automatically detect the character encoding
    AutoDetectReader reader = new AutoDetectReader(new CloseShieldInputStream(stream), metadata, LOADER);
    try {
      metadata.set(Metadata.CONTENT_TYPE, MEDIATYPE_CSV.toString());
      XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);

      CSVReader csvReader = createCSVReader(reader, metadata, context);
      String[] columnNames = csvReader.readNext();
      Map<String, String> record = new LinkedHashMap();
      String[] columnValues;
      while ((columnValues = csvReader.readNext()) != null) {
        record.clear();
        for (int i = 0; i < columnValues.length; i++) {
          record.put(columnNames[i], columnValues[i]);
        }
        process(record, xhtml, metadata, context);
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

  protected CSVReader createCSVReader(AutoDetectReader reader, Metadata metadata, ParseContext context) {
    // TODO: optionally get separator and skipLines and fieldNames from ParseContext and/or Event header?
    int skipLines = 0;
    CSVReader csvReader = new CSVReader(reader, ',', au.com.bytecode.opencsv.CSVParser.DEFAULT_QUOTE_CHARACTER, skipLines);
    return csvReader;
  }

  /** Processes the given record */
  protected void process(Map<String, String> record, XHTMLContentHandler handler, Metadata metadata, ParseContext context)
      throws IOException, SAXException, SolrServerException {     
    LOGGER.debug("record #{}: {}", numRecords, record);
    List<SolrInputDocument> docs = extract(record, handler, metadata, context);
    docs = transform(docs, metadata, context);
    load(docs, metadata, context); 
  }

  /** Extracts zero or more Solr documents from the given record */
  protected List<SolrInputDocument> extract(Map<String, String> record, XHTMLContentHandler handler, Metadata metadata, ParseContext context)
       throws SAXException {
    SolrContentHandler solrHandler = context.get(SolrContentHandler.class);
    handler.startDocument();
    solrHandler.startDocument(); // this is necessary because handler.startDocument() does not delegate all the way down to solrHandler
    handler.startElement("p");
    for (Map.Entry<String, String> field : record.entrySet()) {
      handler.element(field.getKey(), field.getValue());
    }
    handler.endElement("p");
//    handler.endDocument(); // this would cause a bug!
    solrHandler.endDocument();
    SolrInputDocument doc = solrHandler.newDocument().deepCopy();
    return Collections.singletonList(doc);
  }
  
  /** Extension point to transform a list of documents in an application specific way. Does nothing by default */
  protected List<SolrInputDocument> transform(List<SolrInputDocument> docs, Metadata metadata, ParseContext context) {
    return docs;
  }

  /** Loads the given documents into Solr */
  protected void load(List<SolrInputDocument> docs, Metadata metadata, ParseContext context) throws IOException, SolrServerException {
    for (SolrInputDocument doc : docs) {        
      // TODO: figure out name of unique key from IndexSchema
      String id = doc.getFieldValue(UUIDInterceptor.SOLR_ID_HEADER_NAME).toString();
      long num;
      doc.setField(UUIDInterceptor.SOLR_ID_HEADER_NAME, UUIDInterceptor.generateUUID(id, num = numRecords.getAndIncrement()));
      LOGGER.debug("record #{} loading doc: {}", num, doc);
    }
    SimpleSolrSink sink = context.get(SimpleSolrSink.class);
    sink.load(docs);    
  }

//  private static void debugTmp() throws IOException {
//    int skipLines = 0;
//    CSVReader reader = new CSVReader(new FileReader("yourfile.csv"), ',', au.com.bytecode.opencsv.CSVParser.DEFAULT_QUOTE_CHARACTER, skipLines);
//    String[] columns;
//    while ((columns = reader.readNext()) != null) {
//      System.out.println("line: "+ Arrays.asList(columns));
//    }
//    reader.close();
//  }
  
}
