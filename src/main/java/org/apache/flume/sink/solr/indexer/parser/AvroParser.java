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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.apache.flume.sink.solr.indexer.IndexerException;
import org.apache.flume.sink.solr.indexer.ParseInfo;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.handler.extraction.SolrContentHandler;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.XHTMLContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

/**
 * Avro binary format parser that extracts search documents from Avro records
 * (using Apache Tika and Solr Cell) and loads them into Solr.
 */
public class AvroParser extends AbstractParser {

  private static final MediaType MEDIATYPE_AVRO = MediaType.parse("avro/binary"); 
  private static final Set<MediaType> SUPPORTED_TYPES = Collections.singleton(MEDIATYPE_AVRO);
  private static final Logger LOGGER = LoggerFactory.getLogger(AvroParser.class);
  private static final long serialVersionUID = -6656103329236898910L;

  @Override
  public Set<MediaType> getSupportedTypes(ParseContext context) {
    return SUPPORTED_TYPES;
  }

  @Override
  /** Processes the given Avro file and converts records to solr documents and loads them into Solr */
  public void parse(InputStream in, ContentHandler handler, Metadata metadata, ParseContext context)
      throws IOException, SAXException, TikaException {
    try {
      parse2(in, handler, metadata, context);
    } catch (Exception e) {
      LOGGER.error("Cannot parse", e);
      throw new IOException(e);
    }
  }

  protected void parse2(InputStream in, ContentHandler handler, Metadata metadata, ParseContext context)
      throws IOException, SAXException {

    getParseInfo(context).setMultiDocumentParser(true); // TODO hack alert!

    /*
     * Avro requires a SeekableInput so looks like we need to first fetch it all
     * into a buffer. TODO optimize via a new custom SeekableInput impl
     */
    byte[] buf = new byte[4 * 1024];
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    int len;
    while ((len = in.read(buf)) >= 0) {
      bout.write(buf, 0, len);
    }

    metadata.set(Metadata.CONTENT_TYPE, MEDIATYPE_AVRO.toString());
    XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);

    DatumReader<IndexedRecord> datumReader = new GenericDatumReader();
    FileReader<IndexedRecord> reader = null;
    try {
      reader = DataFileReader.openReader(new SeekableByteArrayInput(bout.toByteArray()), datumReader);
      Schema schema = getSchema(reader, context);
      IndexedRecord record = new GenericData.Record(schema);
      while (reader.hasNext()) {
        reader.next(record);
        process(record, xhtml, metadata, context);
      }
    } catch (SolrServerException e) {
      throw new IndexerException(e);
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  protected Schema getSchema(FileReader<IndexedRecord> reader, ParseContext context) {
    return reader.getSchema();
  }

  protected ParseInfo getParseInfo(ParseContext context) {
    return context.get(ParseInfo.class);
  }

  /** Processes the given Avro record */
  protected void process(IndexedRecord record, XHTMLContentHandler handler, Metadata metadata, ParseContext context)
      throws IOException, SAXException, SolrServerException {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("record #{}: {}", getParseInfo(context).getRecordNumber(), record);
    }
    List<SolrInputDocument> docs = extract(record, handler, metadata, context);
    docs = transform(docs, metadata, context);
    load(docs, metadata, context);
  }

  /** Extracts zero or more Solr documents from the given Avro record */
  protected List<SolrInputDocument> extract(IndexedRecord record, XHTMLContentHandler handler, Metadata metadata,
      ParseContext context) throws SAXException {
    SolrContentHandler solrHandler = getParseInfo(context).getSolrContentHandler();
    handler.startDocument();
    solrHandler.startDocument(); // this is necessary because handler.startDocument() does not delegate all the way down to solrHandler
    handler.startElement("p");
    // TODO: optionally also serialize schema?
    serializeToXML(record, record.getSchema(), handler);
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
    getParseInfo(context).getIndexer().load(docs);
  }

  /**
   * Writes the given Avro datum into the given SAX handler, using the given
   * Avro schema
   */
  protected void serializeToXML(Object datum, Schema schema, XHTMLContentHandler handler) throws SAXException {
    // RECORD, ENUM, ARRAY, MAP, UNION, FIXED, STRING, BYTES, INT, LONG, FLOAT,
    // DOUBLE, BOOLEAN, NULL
    switch (schema.getType()) {
    case RECORD: {
      IndexedRecord record = (IndexedRecord) datum;
      handler.startElement("record");
      for (Field field : schema.getFields()) {
        handler.startElement(field.name());
        serializeToXML(record.get(field.pos()), field.schema(), handler);
        handler.endElement(field.name());
      }
      handler.endElement("record");
      break;
    }
    case ENUM: {
      GenericEnumSymbol symbol = (GenericEnumSymbol) datum;
      handler.characters(symbol.toString());
      break;
    }
    case ARRAY: {
      Iterator iter = ((Collection) datum).iterator();
      handler.startElement("array");
      while (iter.hasNext()) {
        handler.startElement("element");
        serializeToXML(iter.next(), schema.getElementType(), handler);
        handler.endElement("element");
      }
      handler.endElement("array");
      break;
    }
    case MAP: {
      Map<CharSequence, ?> map = (Map<CharSequence, ?>) datum;
      handler.startElement("map");
      for (Map.Entry<CharSequence, ?> entry : map.entrySet()) {
        handler.startElement(entry.getKey().toString());
        serializeToXML(entry.getValue(), schema.getValueType(), handler);
        handler.endElement(entry.getKey().toString());
      }
      handler.endElement("map");
      break;
    }
    case UNION: {
      int index = GenericData.get().resolveUnion(schema, datum);
      serializeToXML(datum, schema.getTypes().get(index), handler);
      break;
    }
    case FIXED: {
      GenericFixed fixed = (GenericFixed) datum;
      handler.characters(utf8toString(fixed.bytes()));
      break;
    }
    case BYTES: {
      ByteBuffer buf = (ByteBuffer) datum;
      int pos = buf.position();
      byte[] bytes = new byte[buf.remaining()];
      buf.get(bytes);
      buf.position(pos); // undo relative read
      handler.characters(utf8toString(bytes));
      break;
    }
    case STRING: {
      handler.characters(datum.toString());
      break;
    }
    case INT: {
      handler.characters(datum.toString());
      break;
    }
    case LONG: {
      handler.characters(datum.toString());
      break;
    }
    case FLOAT: {
      handler.characters(datum.toString());
      break;
    }
    case DOUBLE: {
      handler.characters(datum.toString());
      break;
    }
    case BOOLEAN: {
      handler.characters(datum.toString());
      break;
    }
    case NULL: {
      break;
    }
    default:
      throw new AvroRuntimeException("Can't create a: " + schema.getType());
    }
  }

  private String utf8toString(byte[] bytes) {
    try {
      return new String(bytes, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e); // unreachable
    }
  }

}