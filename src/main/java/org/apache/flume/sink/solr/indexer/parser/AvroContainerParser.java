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
import java.util.Collections;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.apache.flume.sink.solr.indexer.IndexerException;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.XHTMLContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

/**
 * Avro binary container file format parser that extracts search documents from
 * Avro records (using Apache Tika and Solr Cell) and loads them into Solr.
 * 
 * The schema for reading is retrieved from the container but the schema can
 * also be overriden.
 */
public class AvroContainerParser extends AvroParser {

  private static final Logger LOGGER = LoggerFactory.getLogger(AvroContainerParser.class);
  private static final long serialVersionUID = -6656103329236898911L;

  public AvroContainerParser() {
    setSupportedTypes(Collections.singleton(MediaType.parse("avro/binary")));
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

    metadata.set(Metadata.CONTENT_TYPE, getSupportedTypes(context).iterator().next().toString());
    XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);

    DatumReader<GenericContainer> datumReader = new GenericDatumReader();
    FileReader<GenericContainer> reader = null;
    try {
      reader = DataFileReader.openReader(new SeekableByteArrayInput(bout.toByteArray()), datumReader);
      Schema schema = getSchema(reader.getSchema(), metadata, context);
      if (schema == null) {
        throw new NullPointerException("Avro schema must not be null");
      }
      IndexedRecord record = new GenericData.Record(schema);
      while (reader.hasNext()) {
        GenericContainer datum = reader.next(record);
        process(datum, xhtml, metadata, context);
      }
    } catch (SolrServerException e) {
      throw new IndexerException(e);
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  @Override
  protected Schema getSchema(Schema schema, Metadata metadata, ParseContext context) {
    return schema;
  }

}
