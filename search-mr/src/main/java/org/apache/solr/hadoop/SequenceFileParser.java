///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//package org.apache.flume.sink.solr.indexer.parser;
//
//import java.io.BufferedReader;
//import java.io.EOFException;
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.InputStreamReader;
//import java.io.UnsupportedEncodingException;
//import java.nio.ByteBuffer;
//import java.util.Collection;
//import java.util.Collections;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//import java.util.Random;
//import java.util.Set;
//
//import org.apache.avro.Schema;
//import org.apache.avro.Schema.Field;
//import org.apache.avro.file.SeekableInput;
//import org.apache.avro.generic.GenericContainer;
//import org.apache.avro.generic.GenericData;
//import org.apache.avro.generic.GenericDatumReader;
//import org.apache.avro.generic.GenericEnumSymbol;
//import org.apache.avro.generic.GenericFixed;
//import org.apache.avro.generic.IndexedRecord;
//import org.apache.avro.io.DatumReader;
//import org.apache.avro.io.Decoder;
//import org.apache.avro.io.DecoderFactory;
//import org.apache.flume.sink.solr.indexer.IndexerException;
//import org.apache.flume.sink.solr.indexer.ParseInfo;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.PositionedReadable;
//import org.apache.hadoop.fs.Seekable;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.SequenceFile;
//import org.apache.hadoop.io.Text;
//import org.apache.solr.client.solrj.SolrServerException;
//import org.apache.solr.common.SolrInputDocument;
//import org.apache.solr.handler.extraction.SolrContentHandler;
//import org.apache.tika.exception.TikaException;
//import org.apache.tika.metadata.Metadata;
//import org.apache.tika.mime.MediaType;
//import org.apache.tika.parser.AbstractParser;
//import org.apache.tika.parser.ParseContext;
//import org.apache.tika.sax.XHTMLContentHandler;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.xml.sax.ContentHandler;
//import org.xml.sax.SAXException;
//
//import com.fasterxml.jackson.core.JsonParseException;
//import com.fasterxml.jackson.databind.JsonNode;
//import com.fasterxml.jackson.databind.ObjectMapper;
//
///**
// * FIXME
// * Avro parser that extracts search documents from Avro records (using Apache
// * Tika and Solr Cell) and loads them into Solr.
// * 
// * The schema for reading must be explicitly supplied.
// */
//public class SequenceFileParser extends AbstractParser {
//
//  private Set<MediaType> supportedMediaTypes;
//
//  private static final Logger LOGGER = LoggerFactory.getLogger(SequenceFileParser.class);
//  private static final long serialVersionUID = -6656103329236898910L;
//
//  public SequenceFileParser() {
//    setSupportedTypes(Collections.singleton(MediaType.parse("application/sequencefile")));
//  }
//  
//  @Override
//  public Set<MediaType> getSupportedTypes(ParseContext context) {
//    return supportedMediaTypes;
//  }
//
//  public void setSupportedTypes(Set<MediaType> supportedMediaTypes) {
//    this.supportedMediaTypes = supportedMediaTypes;
//  }
//
//  @Override
//  /** Processes the given file and converts records to solr documents and loads them into Solr */
//  public void parse(InputStream in, ContentHandler handler, Metadata metadata, ParseContext context)
//      throws IOException, SAXException, TikaException {
//    try {
//      parse2(in, handler, metadata, context);
//    } catch (Exception e) {
//      LOGGER.error("Cannot parse", e);
//      throw new IOException(e);
//    }
//  }
//
//  private void parse2(InputStream in, ContentHandler handler, Metadata metadata, ParseContext context)
//      throws IOException, SolrServerException {
//
//    ParseInfo parseInfo = getParseInfo(context); 
//    parseInfo.setMultiDocumentParser(true); // TODO hack alert!
//
//    long numRecords = 0;
//    
//    SequenceFile.Reader reader =
//        new SequenceFile.Reader(FileSystem.get(config), path, config);
//    Text key = (Text) reader.getKeyClass().newInstance();
//    IntWritable value = (IntWritable) reader.getValueClass().newInstance();
//    while (reader.next(key, value)) {
//        // do something here
//    }
//    reader.close();
////    ObjectMapper mapper = new ObjectMapper();
////    BufferedReader reader = new BufferedReader(new InputStreamReader(in, "UTF-8"));
//    try {
//      while (reader.next(key, value)) {
//        // do something here
////        LOGGER.debug("doc: {}", doc);
////        parseInfo.getIndexer().load(Collections.singletonList(doc));
//        numRecords++;
//      }
//      
////        String json = nextLine(reader);
////        if (json == null) {
////          break;
////        }
////  
////        JsonNode rootNode;
////        try {
////          // src can be a File, URL, InputStream, etc
////          rootNode = mapper.readValue(json, JsonNode.class); 
////        } catch (JsonParseException e) {
////          LOGGER.info("json parse exception after " + numRecords + " records");
////          LOGGER.debug("json parse exception after " + numRecords + " records", e);
////          break;
////        }
////    
////        SolrInputDocument doc = new SolrInputDocument();
////        JsonNode user = rootNode.get("user");
////        JsonNode idNode = rootNode.get("id_str");
////        if (idNode == null || idNode.textValue() == null) {
////          continue; // skip
////        }
////    
////        doc.addField("id", idPrefix + idNode.textValue());
//////        tryAddDate(doc, "created_at", rootNode.get("created_at"));
//////        tryAddString(doc, "source", rootNode.get("source"));
//////        tryAddString(doc, "text", rootNode.get("text"));
//////        tryAddInt(doc, "retweet_count", rootNode.get("retweet_count"));
//////        tryAddBool(doc, "retweeted", rootNode.get("retweeted"));
//////        tryAddLong(doc, "in_reply_to_user_id", rootNode.get("in_reply_to_user_id"));
//////        tryAddLong(doc, "in_reply_to_status_id", rootNode.get("in_reply_to_status_id"));
//////        tryAddString(doc, "media_url_https", rootNode.get("media_url_https"));
//////        tryAddString(doc, "expanded_url", rootNode.get("expanded_url"));
//////    
//////        tryAddInt(doc, "user_friends_count", user.get("friends_count"));
//////        tryAddString(doc, "user_location", user.get("location"));
//////        tryAddString(doc, "user_description", user.get("description"));
//////        tryAddInt(doc, "user_statuses_count", user.get("statuses_count"));
//////        tryAddInt(doc, "user_followers_count", user.get("followers_count"));
//////        tryAddString(doc, "user_screen_name", user.get("screen_name"));
//////        tryAddString(doc, "user_name", user.get("name"));
////        
//    } finally {
//      LOGGER.info("processed {} records", numRecords);
//    }
//  }
//  
//  private ParseInfo getParseInfo(ParseContext context) {
//    return context.get(ParseInfo.class);
//  }
//  
//  
//  ///////////////////////////////////////////////////////////////////////////////
//  // Nested classes:
//  ///////////////////////////////////////////////////////////////////////////////
//
//  /**
//   * A {@link Seekable} backed by an {@link InputStream} that can only advance
//   * forward, not backwards.
//   */
//  public static final class ForwardOnlySeekableInputStream implements Seekable, PositionedReadable {
//    // class is public for testing only!
//    
//    private final InputStream in;
//    private long pos = 0;
//    
//    public ForwardOnlySeekableInputStream(InputStream in) {
//      this.in = in;
//    }
//
//    @Override
//    public long getPos() throws IOException {
//      return pos;
//    }
//
//    @Override
//    public void seek(long p) throws IOException {
//      long todo = p - pos;
//      if (todo < 0) {
//        throw new UnsupportedOperationException("Seeking backwards is not supported");
//      }
//      skip(todo);
//    }
//
//    private long skip(long len) throws IOException {
//      // borrowed from org.apache.hadoop.io.IOUtils.skipFully()
//      len = Math.max(0, len);
//      long todo = len;
//      while (todo > 0) {
//        long ret = in.skip(todo);
//        if (ret == 0) {
//          // skip may return 0 even if we're not at EOF.  Luckily, we can 
//          // use the read() method to figure out if we're at the end.
//          int b = in.read();
//          if (b == -1) {
//            throw new EOFException( "Premature EOF from inputStream after " +
//                "skipping " + (len - todo) + " byte(s).");
//          }
//          ret = 1;
//        }
//        todo -= ret;
//        pos += ret;
//      }
//      return len;
//    }
//    
//    @Override
//    public boolean seekToNewSource(long targetPos) throws IOException {
//      throw new UnsupportedOperationException();
//    }
//    
//  }
//
//}
