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

import java.io.EOFException;
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
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
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
public abstract class AvroParser extends AbstractParser {

  private Set<MediaType> supportedMediaTypes;

  private static final Logger LOGGER = LoggerFactory.getLogger(AvroParser.class);
  private static final long serialVersionUID = -6656103329236898910L;

  public AvroParser() {
    setSupportedTypes(Collections.singleton(MediaType.parse("avro/unknown+schemaless")));
  }
  
  /** Returns the Avro schema to use for reading */
  protected abstract Schema getSchema(Schema schema, Metadata metadata, ParseContext context);

  @Override
  public Set<MediaType> getSupportedTypes(ParseContext context) {
    return supportedMediaTypes;
  }

  public void setSupportedTypes(Set<MediaType> supportedMediaTypes) {
    this.supportedMediaTypes = supportedMediaTypes;
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

  protected boolean isJSON() {
    return false;
  }
  
  protected void parse2(InputStream in, ContentHandler handler, Metadata metadata, ParseContext context)
      throws IOException, SAXException {

    getParseInfo(context).setMultiDocumentParser(true); // TODO hack alert!

    metadata.set(Metadata.CONTENT_TYPE, getSupportedTypes(context).iterator().next().toString());
    XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);

    Schema schema = getSchema(null, metadata, context);
    if (schema == null) {
      throw new NullPointerException("Avro schema must not be null");
    }
    DatumReader<GenericContainer> datumReader = new GenericDatumReader<GenericContainer>(schema);
    
    Decoder decoder;
    if (isJSON()) {
      decoder = DecoderFactory.get().jsonDecoder(schema, in);
    } else {
      decoder = DecoderFactory.get().binaryDecoder(in, null);
    }
    
    try {
      IndexedRecord record = new GenericData.Record(schema);
      while (true) {
        GenericContainer datum = datumReader.read(record, decoder);
        process(datum, xhtml, metadata, context);
      }
    } catch (EOFException e) { 
      ; // ignore
    } catch (SolrServerException e) {
      throw new IndexerException(e);
    } finally {
      in.close();
    }
  }

  protected ParseInfo getParseInfo(ParseContext context) {
    return context.get(ParseInfo.class);
  }

  /** Processes the given Avro record */
  protected void process(GenericContainer record, XHTMLContentHandler handler, Metadata metadata, ParseContext context)
      throws IOException, SAXException, SolrServerException {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("record #{}: {}", getParseInfo(context).getRecordNumber(), record);
    }
    List<SolrInputDocument> docs = extract(record, handler, metadata, context);
    docs = transform(docs, metadata, context);
    load(docs, metadata, context);
  }

  /** Extracts zero or more Solr documents from the given Avro record */
  protected List<SolrInputDocument> extract(GenericContainer record, XHTMLContentHandler handler, Metadata metadata,
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
