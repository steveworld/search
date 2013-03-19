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

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.tika.ParseInfo;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.sax.XHTMLContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

/**
 * Avro binary container file format parser that extracts search documents from
 * Avro records (using Apache Tika and Solr Cell) and loads them into Solr.
 * 
 * The schema for reading is retrieved from the container but the schema can
 * also be overriden.
 */
public class StreamingAvroContainerParser extends StreamingAvroParser {

  public StreamingAvroContainerParser() {
  }
  
  @Override
  protected void doParse(InputStream in, ContentHandler handler) throws IOException, SAXException, SolrServerException {
    ParseInfo info = getParseInfo();
    info.setMultiDocumentParser(true);
    Metadata metadata = info.getMetadata();
    metadata.set(Metadata.CONTENT_TYPE, getSupportedTypes(info.getParseContext()).iterator().next().toString());
    XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);

    DatumReader<GenericContainer> datumReader = new GenericDatumReader();
    FileReader<GenericContainer> reader = null;
    try {
      reader = new DataFileReader(new ForwardOnlySeekableInputStream(in), datumReader);
      Schema schema = getSchema(reader.getSchema());
      if (schema == null) {
        throw new NullPointerException("Avro schema must not be null");
      }
      GenericContainer datum = new GenericData.Record(schema);
      while (reader.hasNext()) {
        datum = reader.next(datum);
        process(datum, xhtml);
      }
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  @Override
  protected Schema getSchema(Schema schema) {
    return schema;
  }

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////

  /**
   * A {@link SeekableInput} backed by an {@link InputStream} that can only advance
   * forward, not backwards.
   */
  public static final class ForwardOnlySeekableInputStream implements SeekableInput {
    // class is public for testing only!
    
    private final InputStream in;
    private long pos = 0;
    
    public ForwardOnlySeekableInputStream(InputStream in) {
      this.in = in;
    }

    @Override
    public long tell() throws IOException {
      return pos;
    }
    
    @Override
    public int read(byte b[], int off, int len) throws IOException {
      int n = in.read(b, off, len);
      if (n > 0) {
        pos += n;
      }
      return n;
    }
    
    @Override
    public long length() throws IOException {
      throw new UnsupportedOperationException("Random access is not supported");
    }

    @Override
    public void seek(long p) throws IOException {
      long todo = p - pos;
      if (todo < 0) {
        throw new UnsupportedOperationException("Seeking backwards is not supported");
      }
      skip(todo);
    }

    private long skip(long len) throws IOException {
      // borrowed from org.apache.hadoop.io.IOUtils.skipFully()
      len = Math.max(0, len);
      long todo = len;
      while (todo > 0) {
        long ret = in.skip(todo);
        if (ret == 0) {
          // skip may return 0 even if we're not at EOF.  Luckily, we can 
          // use the read() method to figure out if we're at the end.
          int b = in.read();
          if (b == -1) {
            throw new EOFException( "Premature EOF from inputStream after " +
                "skipping " + (len - todo) + " byte(s).");
          }
          ret = 1;
        }
        todo -= ret;
        pos += ret;
      }
      return len;
    }
    
    @Override
    public void close() throws IOException {
      in.close();
    }
    
  }

}
