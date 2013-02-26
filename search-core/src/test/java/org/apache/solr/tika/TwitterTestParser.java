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
package org.apache.solr.tika;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Random;
import java.util.Set;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.tika.ParseInfo;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Tika JSON parser that extracts search documents from twitter tweets obtained from the twitter 1% sample firehose with the delimited=length option.
 * For background see https://dev.twitter.com/docs/api/1.1/get/statuses/sample.
 * Example to download data from the twitter 1% sample firehose:
 * <pre>
 * while [ 1 ]; do echo `date`; curl https://stream.twitter.com/1/statuses/sample.json?delimited=length -u$userName:$password > sample-statuses-$(date +%Y%m%d-%H%M%S); sleep 10; done
 * </pre>
 * The JSON input format is documented at https://dev.twitter.com/docs/platform-objects/tweets
 */
public class TwitterTestParser extends AbstractParser {

  private Set<MediaType> supportedMediaTypes;

  // Fri May 14 02:52:55 +0000 2010
  private SimpleDateFormat formatterFrom = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
  private SimpleDateFormat formatterTo = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

  private static final Logger LOGGER = LoggerFactory.getLogger(TwitterTestParser.class);
  private static final long serialVersionUID = -6656003329236898910L;

  public TwitterTestParser() {
    setSupportedTypes(Collections.singleton(MediaType.parse("mytwittertest/json+delimited+length")));
  }
  
  @Override
  public Set<MediaType> getSupportedTypes(ParseContext context) {
    return supportedMediaTypes;
  }

  public void setSupportedTypes(Set<MediaType> supportedMediaTypes) {
    this.supportedMediaTypes = supportedMediaTypes;
  }

  @Override
  /** Processes the given file and converts records to solr documents and loads them into Solr */
  public void parse(InputStream in, ContentHandler handler, Metadata metadata, ParseContext context)
      throws IOException, SAXException, TikaException {
    try {
      parse2(in, handler, metadata, context);
    } catch (Exception e) {
      LOGGER.error("Cannot parse", e);
      throw new IOException(e);
    }
  }

  private void parse2(InputStream in, ContentHandler handler, Metadata metadata, ParseContext context)
      throws IOException, SolrServerException {

    ParseInfo parseInfo = getParseInfo(context); 
    parseInfo.setMultiDocumentParser(true); // TODO hack alert!

    String idPrefix = System.getProperty("idPrefix");
    if ("random".equals(idPrefix)) {
      idPrefix = String.valueOf(new Random().nextInt());
    } else if (idPrefix == null) {
      idPrefix = "";
    }
    
    long numRecords = 0;
    ObjectMapper mapper = new ObjectMapper();
    BufferedReader reader = new BufferedReader(new InputStreamReader(in, "UTF-8"));
    try {
      while (true) {
        String json = nextLine(reader);
        if (json == null) {
          break;
        }
  
        JsonNode rootNode;
        try {
          // src can be a File, URL, InputStream, etc
          rootNode = mapper.readValue(json, JsonNode.class); 
        } catch (JsonParseException e) {
          LOGGER.info("json parse exception after " + numRecords + " records");
          LOGGER.debug("json parse exception after " + numRecords + " records", e);
          break;
        }
    
        SolrInputDocument doc = new SolrInputDocument();
        JsonNode user = rootNode.get("user");
        JsonNode idNode = rootNode.get("id_str");
        if (idNode == null || idNode.textValue() == null) {
          continue; // skip
        }
    
        doc.addField("id", idPrefix + idNode.textValue());
        tryAddDate(doc, "created_at", rootNode.get("created_at"));
        tryAddString(doc, "source", rootNode.get("source"));
        tryAddString(doc, "text", rootNode.get("text"));
        tryAddInt(doc, "retweet_count", rootNode.get("retweet_count"));
        tryAddBool(doc, "retweeted", rootNode.get("retweeted"));
        tryAddLong(doc, "in_reply_to_user_id", rootNode.get("in_reply_to_user_id"));
        tryAddLong(doc, "in_reply_to_status_id", rootNode.get("in_reply_to_status_id"));
        tryAddString(doc, "media_url_https", rootNode.get("media_url_https"));
        tryAddString(doc, "expanded_url", rootNode.get("expanded_url"));
    
        tryAddInt(doc, "user_friends_count", user.get("friends_count"));
        tryAddString(doc, "user_location", user.get("location"));
        tryAddString(doc, "user_description", user.get("description"));
        tryAddInt(doc, "user_statuses_count", user.get("statuses_count"));
        tryAddInt(doc, "user_followers_count", user.get("followers_count"));
        tryAddString(doc, "user_screen_name", user.get("screen_name"));
        tryAddString(doc, "user_name", user.get("name"));
        
        LOGGER.debug("tweetdoc: {}", doc);
        parseInfo.getIndexer().load(Collections.singletonList(doc));
        numRecords++;
      }
    } finally {
      LOGGER.info("processed {} records", numRecords);
    }
  }

  private String nextLine(BufferedReader reader) throws IOException {
    String line;
    while ((line = reader.readLine()) != null) {
      if (line.length() > 0)
        break; // ignore empty lines
    }
    if (line == null)
      return null;
    Integer.parseInt(line); // sanity check

    while ((line = reader.readLine()) != null) {
      if (line.length() > 0)
        break; // ignore empty lines
    }
    return line;
  }

  private void tryAddDate(SolrInputDocument doc, String solr_field, JsonNode node) {
    if (node == null)
      return;
    String val = node.asText();
    if (val == null) {
      return;
    }
    try {
//      String tmp = formatterTo.format(formatterFrom.parse(val.trim()));
      doc.addField(solr_field, formatterTo.format(formatterFrom.parse(val.trim())));
    } catch (Exception e) {
      LOGGER.error("Could not parse date " + val);
//      ++exceptionCount;
    }
  }

  private void tryAddLong(SolrInputDocument doc, String solr_field, JsonNode node) {
    if (node == null)
      return;
    Long val = node.asLong();
    if (val == null) {
      return;
    }
    doc.addField(solr_field, val);
  }

  private void tryAddInt(SolrInputDocument doc, String solr_field, JsonNode node) {
    if (node == null)
      return;
    Integer val = node.asInt();
    if (val == null) {
      return;
    }
    doc.addField(solr_field, val);
  }

  private void tryAddBool(SolrInputDocument doc, String solr_field, JsonNode node) {
    if (node == null)
      return;
    Boolean val = node.asBoolean();
    if (val == null) {
      return;
    }
    doc.addField(solr_field, val);
  }

  private void tryAddString(SolrInputDocument doc, String solr_field, JsonNode node) {
    if (node == null)
      return;
    String val = node.asText();
    if (val == null) {
      return;
    }
    doc.addField(solr_field, val);
  }

  private ParseInfo getParseInfo(ParseContext context) {
    return ParseInfo.getParseInfo(context);
  }

}
