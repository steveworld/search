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
import java.util.List;
import java.util.Set;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.tika.ParseInfo;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

/**
 * Abstract base class for convenient implementation of Tika parsers that stream
 * multiple SolrInputDocuments per input stream into Solr. Examples: CSV, Avro
 */
public abstract class AbstractStreamingParser implements Parser {

  private Set<MediaType> supportedMediaTypes;
  private ParseInfo parseInfo;

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractStreamingParser.class);

  public Set<MediaType> getSupportedTypes(ParseContext context) {
    return supportedMediaTypes;
  }

  public void setSupportedTypes(Set<MediaType> supportedMediaTypes) {
    this.supportedMediaTypes = supportedMediaTypes;
  }

  protected final ParseInfo getParseInfo() {
    assert parseInfo != null;
    return parseInfo;
  }

  /** Loads the given documents into Solr */
  protected void load(List<SolrInputDocument> docs) throws IOException, SolrServerException {
    parseInfo.getIndexer().load(docs);
  }

  @Override
  /** Parses the given input stream and converts it to Solr documents and loads them into Solr */
  public void parse(InputStream in, ContentHandler handler, Metadata metadata, ParseContext parseContext)
      throws IOException, SAXException, TikaException {
    
    parseInfo = ParseInfo.getParseInfo(parseContext);
    try {
      doParse(in, handler);
    } catch (Exception e) {
      LOGGER.error("Cannot parse", e);
      if (e instanceof IOException) {
        throw (IOException) e;
      } else if (e instanceof SAXException) {
        throw (SAXException) e;
      } else if (e instanceof TikaException) {
        throw (TikaException) e;
      }
      throw new TikaException("Cannot parse", e);
    }
  }

  /** Parses the given input stream and converts it to Solr documents and loads them into Solr */
  protected abstract void doParse(InputStream in, ContentHandler handler) 
      throws IOException, SAXException, TikaException, SolrServerException;

}
