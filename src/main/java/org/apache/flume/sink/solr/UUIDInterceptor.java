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


import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.uuid.EthernetAddress;
import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.RandomBasedGenerator;
import com.fasterxml.uuid.impl.TimeBasedGenerator;

/** Flume interceptor that adds a unique id to the set of Flume event headers */
public class UUIDInterceptor implements Interceptor {

  private final RandomBasedGenerator randomBasedUuidGenerator = Generators.randomBasedGenerator();
  private final TimeBasedGenerator timeBasedUuidGenerator = Generators.timeBasedGenerator(EthernetAddress.fromInterface());

  public static final String SOLR_ID_HEADER_NAME = "id"; // TODO: make this configurable via context
  
  private static final Logger LOGGER = LoggerFactory.getLogger(UUIDInterceptor.class);
  
  private UUIDInterceptor(Context context) {
  }

  @Override
  public void initialize() {
  }

  protected String generateUUID() {
    return "ss#" + timeBasedUuidGenerator.generate().toString() + "#" + randomBasedUuidGenerator.generate().toString();
    //return UUID.randomUUID().toString();
  }
  
  public static String generateUUID(String prefix, long sequenceNumber) {
    return prefix + "#" + sequenceNumber;
  }
  
  @Override
  public Event intercept(Event event) {
    Map<String, String> headers = event.getHeaders();
    if (!headers.containsKey(SOLR_ID_HEADER_NAME)) {
      headers.put(SOLR_ID_HEADER_NAME, generateUUID());
    }
    return event;
  }
  
  @Override
  public List<Event> intercept(List<Event> events) {
    List results = new ArrayList(events.size());
    for (Event event : events) {
      event = intercept(event);
      if (event != null) {
        results.add(event);
      }      
    }
    return results;
  }

  @Override
  public void close() {
  }

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  /** Builder implementations MUST have a public no-arg constructor */
  public static class Builder implements Interceptor.Builder {

    private Context context;
    
    public Builder() {}
    
    @Override
    public Interceptor build() {
      return new UUIDInterceptor(context);
    }

    @Override
    public void configure(Context context) {
      this.context = context;
//      String tikaConfigLoc = context.getString("foo.bar");
      /// ...
    }
    
  }

}
