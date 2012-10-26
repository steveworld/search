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

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.AbstractHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Demo Flume source that connects via HTTP to the 1% sample twitter firehose, continously downloads tweets, converts
 * them to Avro format and sends Avro events to a downstream Flume sink.
 * 
 * Requires the username and password of a normal twitter user account.
 */
public class TwitterSource extends AbstractSource implements EventDrivenSource, Configurable {

  private String url = "https://stream.twitter.com/1/statuses/sample.json?delimited=length";
  private Credentials userNameAndPassword;
  private int batchSize = 100;

  private CountDownLatch isStopping = new CountDownLatch(0);
  
  private long docCount = 0;
  private long startTime = 0;
  private long exceptionCount = 0;
  private long totalTextIndexed = 0;
  private long skippedDocs = 0;
  private long rawBytes = 0;
  
  // Fri May 14 02:52:55 +0000 2010
  private SimpleDateFormat _formatterFrom = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
  private SimpleDateFormat _formatterTo = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
  private DecimalFormat numFormatter = new DecimalFormat("###,###.###");

  private static int REPORT_INTERVAL = 100;
  private static int STATS_INTERVAL = REPORT_INTERVAL * 10;
  private static final Logger LOGGER = LoggerFactory.getLogger(TwitterSource.class);

  public TwitterSource() {
  }

  @Override
  public void configure(Context context) {
    String userName = context.getString("username");
    String password = context.getString("password");
    userNameAndPassword = new UsernamePasswordCredentials(userName, password);
    url = context.getString("url", url);
    batchSize = context.getInteger("batchSize", batchSize);
  }
  
  @Override
  public synchronized void start() {
    super.start();
    if (isStopping.getCount() <= 0) {
      LOGGER.info("Starting twitter source {} ...", this);
      isStopping = new CountDownLatch(1);
      new Thread(new Runnable() {
        public void run() {
          doRun();
        }      
      }).start();
      LOGGER.info("Twitter source {} started.", getName());
    }
  }

  @Override
  public synchronized void stop() {
    LOGGER.info("Twitter source {} stopping...", getName());
    isStopping.countDown();
    super.stop();
    LOGGER.info("Twitter source {} stopped.", getName());
  }

  private void doRun() {
    docCount = 0;
    startTime = System.currentTimeMillis();
    exceptionCount = 0;
    totalTextIndexed = 0;
    skippedDocs = 0;
    rawBytes = 0;
    AbstractHttpClient httpClient = new DefaultHttpClient(); // uses HTTP persistent connections by default, uses TCP_NODELAY = true by default
    httpClient.getCredentialsProvider().setCredentials(AuthScope.ANY, userNameAndPassword);
    //httpClient.getParams().setParameter(CoreConnectionPNames.TCP_NODELAY, false);  

    while (true) {
      try {
        Schema avroSchema = createAvroSchema();    
        ObjectMapper mapper = new ObjectMapper();
        HttpGet httpGet = new HttpGet(url);
        HttpResponse response = httpClient.execute(httpGet);      
        //int status = response.getStatusLine().getStatusCode();
        HttpEntity entity = response.getEntity();
        if (entity != null) {
          InputStream in = entity.getContent();
          try {
            List<Record> docList = new ArrayList();
            BufferedReader reader = new BufferedReader(new InputStreamReader(in, "UTF-8"));          
            while (true) {
              if (isStopping.await(0, TimeUnit.NANOSECONDS)) {
                // Shut down the connection manager to ensure immediate deallocation of all system resources
                try {
                  httpGet.abort();
                  httpClient.getConnectionManager().shutdown();
                  in = null;
                } catch (Exception e) {
                  LOGGER.error("Cannot shutdown properly", e);
                }
                return;
              }
              String json = nextLine(reader);
              if (json == null) {
                break;
              }
              rawBytes += json.length();
              
              JsonNode rootNode = mapper.readValue(json, JsonNode.class); // src can be a File, URL, InputStream etc  
              Record doc = extractRecord("", avroSchema, rootNode);
              if (doc == null) {
                continue; // skip
              }
              
              docList.add(doc);            
              if (docList.size() >= batchSize) {
                byte[] bytes = serializeToAvro(avroSchema, docList);              
                Event event = EventBuilder.withBody(bytes);
                getChannelProcessor().processEvent(event); // send event to downstream flume sink    
                docList.clear();
              }
              
              docCount++;
              if ((docCount % REPORT_INTERVAL) == 0) {
                LOGGER.info(String.format("Processed %s docs", numFormatter.format(docCount)));
              }
              if ((docCount % STATS_INTERVAL) == 0) {
                logStats();
              }
            }    
          } catch (IOException ex) {
            // In case of an IOException the connection will be released
            // back to the connection manager automatically
            throw ex;
          } catch (RuntimeException ex) {
            // In case of an unexpected exception you may want to abort
            // the HTTP request in order to shut down the underlying
            // connection and release it back to the connection manager.
            httpGet.abort();
            throw ex;
          } finally {
            // Closing the input stream will trigger connection release
            if (in != null) {
              in.close();
            }
          }
        }
      } catch (Exception e) {
        exceptionCount++;
        LOGGER.error("Oops", e); // log and continue
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e1) {
          ; // ignore
        }
      }
    }
  }

  private byte[] serializeToAvro(Schema avroSchema, List<Record> docList) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(avroSchema);
    DataFileWriter<GenericRecord>  dataFileWriter = new DataFileWriter<GenericRecord>(writer);
    dataFileWriter.create(avroSchema, out);       
    for (Record doc2 : docList) {
      dataFileWriter.append(doc2);
    }
    dataFileWriter.close();
    return out.toByteArray();
  }

  private Record extractRecord(String idPrefix, Schema avroSchema, JsonNode rootNode) {
    JsonNode user = rootNode.get("user");
    JsonNode idNode = rootNode.get("id_str");    
    if (idNode == null || idNode.textValue() == null) {
      skippedDocs++;
      return null; // skip
    }
    Record doc = new Record(avroSchema);
    doc.put("id", idPrefix + idNode.textValue());
    tryAddDate(doc, "created_at", rootNode.get("created_at"));
    tryAddString(doc, "source", rootNode.get("source"));
    tryAddString(doc, "text", rootNode.get("text"));
    tryAddInt(doc, "retweet_count", rootNode.get("retweet_count"));
    tryAddBool(doc, "retweeted", rootNode.get("retweeted"));
    tryAddLong(doc, "in_reply_to_user_id", rootNode.get("in_reply_to_user_id"));
    tryAddLong(doc, "in_reply_to_status_id", rootNode.get("in_reply_to_status_id"));
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
    return doc;
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
  
  private Schema createAvroSchema() {
    Schema avroSchema = Schema.createRecord("Doc", "adoc", null, false);
    List<Field> fields = new ArrayList<Field>();
    fields.add(new Field("id", Schema.create(Type.STRING), null, null));
    fields.add(new Field("user_friends_count", createOptional(Schema.create(Type.INT)), null, null));
    fields.add(new Field("user_location", createOptional(Schema.create(Type.STRING)), null, null));
    fields.add(new Field("user_description", createOptional(Schema.create(Type.STRING)), null, null));
    fields.add(new Field("user_statuses_count", createOptional(Schema.create(Type.INT)), null, null));
    fields.add(new Field("user_followers_count", createOptional(Schema.create(Type.INT)), null, null));
    fields.add(new Field("user_name", createOptional(Schema.create(Type.STRING)), null, null));
    fields.add(new Field("user_screen_name", createOptional(Schema.create(Type.STRING)), null, null));
    fields.add(new Field("created_at", createOptional(Schema.create(Type.STRING)), null, null));
    fields.add(new Field("text", createOptional(Schema.create(Type.STRING)), null, null));
    fields.add(new Field("retweet_count", createOptional(Schema.create(Type.INT)), null, null));
    fields.add(new Field("retweeted", createOptional(Schema.create(Type.BOOLEAN)), null, null));
    fields.add(new Field("in_reply_to_user_id", createOptional(Schema.create(Type.LONG)), null, null));
    fields.add(new Field("source", createOptional(Schema.create(Type.STRING)), null, null));
    fields.add(new Field("in_reply_to_status_id", createOptional(Schema.create(Type.LONG)), null, null));
    fields.add(new Field("media_url_https", createOptional(Schema.create(Type.STRING)), null, null));
    fields.add(new Field("expanded_url", createOptional(Schema.create(Type.STRING)), null, null));
    avroSchema.setFields(fields);
    return avroSchema;
  }

  private Schema createOptional(Schema schema) {
    return Schema.createUnion(Arrays.asList(new Schema[] { schema, Schema.create(Type.NULL) }));
  }

  private void tryAddDate(Record doc, String solr_field, JsonNode node) {
    if (node == null)
      return;
    String val = node.asText();
    if (val == null) {
      return;
    }
    try {
      doc.put(solr_field, _formatterTo.format(_formatterFrom.parse(val.trim())));
    } catch (Exception e) {
      LOGGER.warn("Could not parse date " + val);
      exceptionCount++;
    }
  }

  private void tryAddLong(Record doc, String solr_field, JsonNode node) {
    if (node == null)
      return;
    Long val = node.asLong();
    if (val == null) {
      return;
    }
    doc.put(solr_field, val);
  }

  private void tryAddInt(Record doc, String solr_field, JsonNode node) {
    if (node == null)
      return;
    Integer val = node.asInt();
    if (val == null) {
      return;
    }
    doc.put(solr_field, val);
  }

  private void tryAddBool(Record doc, String solr_field, JsonNode node) {
    if (node == null)
      return;
    Boolean val = node.asBoolean();
    if (val == null) {
      return;
    }
    doc.put(solr_field, val);
  }

  private void tryAddString(Record doc, String solr_field, JsonNode node) {
    if (node == null)
      return;
    String val = node.asText();
    if (val == null) {
      return;
    }
    doc.put(solr_field, val);
    totalTextIndexed += val.length();
  }
  
  private void logStats() {    
    double mbIndexed = totalTextIndexed / (1024 * 1024.0);
    long seconds = (System.currentTimeMillis() - startTime) / 1000;
    seconds = Math.max(seconds, 1);
    LOGGER.info(String.format("Total docs indexed: %s, total skipped docs: %s", numFormatter.format(docCount),
        numFormatter.format(skippedDocs)));
    LOGGER.info(String.format("    %s docs/second", numFormatter.format(docCount / seconds)));
    LOGGER.info(String.format("Run took %s seconds and processed:", numFormatter.format(seconds)));
    LOGGER.info(String.format("    %s MB/sec sent to index",
        numFormatter.format(((float) totalTextIndexed / (1024 * 1024)) / seconds)));
    LOGGER.info(String.format("    %s MB text sent to index", numFormatter.format(mbIndexed)));
    LOGGER.info(String.format("    %s MB raw text read", numFormatter.format(rawBytes / (1024 * 1024))));
    LOGGER.info(String.format("There were %s exceptions ignored: ", numFormatter.format(exceptionCount)));
  }

}
