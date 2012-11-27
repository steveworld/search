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
import java.util.Collections;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
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
  
  @Override
  protected void parse2(InputStream in, ContentHandler handler, Metadata metadata, ParseContext context)
      throws IOException, SAXException {

    getParseInfo(context).setMultiDocumentParser(true); // TODO hack alert!
    metadata.set(Metadata.CONTENT_TYPE, getSupportedTypes(context).iterator().next().toString());
    XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);

    DatumReader<GenericContainer> datumReader = new GenericDatumReader();
    FileReader<GenericContainer> reader = null;
    try {
      reader = new DataFileReader(new ForwardOnlySeekableInputStream(in), datumReader);
      Schema schema = getSchema(reader.getSchema(), metadata, context);
      if (schema == null) {
        throw new NullPointerException("Avro schema must not be null");
      }
      GenericContainer datum = new GenericData.Record(schema);
      while (reader.hasNext()) {
        datum = reader.next(datum);
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

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////

  /**
   * A {@link SeekableInput} backed by an {@link InputStream} that can only advance
   * forward, not backwards.
   */
  public static final class ForwardOnlySeekableInputStream extends InputStream implements SeekableInput {
    // class is public for testing only!
    
    private final InputStream in;
    private long pos = 0;
    private long mark = -1;
    
    public ForwardOnlySeekableInputStream(InputStream in) {
      this.in = in;
    }

    @Override
    public long tell() throws IOException {
      return pos;
    }
    
    @Override
    public void seek(long p) throws IOException {
      long todo = p - pos;
      if (todo < 0) {
        throw new UnsupportedOperationException("Seeking backwards is not supported");
      }
      skip(todo);
    }

    @Override
    public long length() throws IOException {
      throw new UnsupportedOperationException("Random access is not supported");
    }

    @Override
    public int read() throws IOException {
      int result = in.read();
      pos++;
      return result;
    }
    
    @Override
    public int read(byte b[]) throws IOException {
      return read(b, 0, b.length);
    }
    
    @Override
    public int read(byte b[], int off, int len) throws IOException {
      int n = in.read(b, off, len);
      pos += n;
      return n;
    }
    
    @Override
    public long skip(long len) throws IOException {
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
    public int available() throws IOException {
      return in.available();
    }

    @Override
    public void close() throws IOException {
      in.close();
    }

    @Override
    public boolean markSupported() {
      return in.markSupported();
    }
    
    @Override
    public void mark(int readLimit) {
      in.mark(readLimit);
      mark = pos;
    }
    
    @Override
    public void reset() throws IOException {
      in.reset();
      pos = mark;
   }
    
  }

}
