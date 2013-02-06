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
package org.apache.flume.sink.solr.indexer;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Set of name-value pairs plus a payload in the form of an InputStream.
 */
public class StreamEvent {

  private final Map<String, String> headers;
  private final InputStream body;

  public StreamEvent() {
    this(null, new HashMap<String, String>());
  }

  public StreamEvent(InputStream body, Map<String, String> headers) {
    if (headers == null) {
      throw new IllegalArgumentException("Headers must not be null");      
    }
    if (body == null) {
      body = new ByteArrayInputStream(new byte[0]);
    }
    this.headers = headers;
    this.body = body;
  }

  public Map<String, String> getHeaders() {
    return headers;
  }

  public InputStream getBody() {
    return body;
  }

  @Override
  public String toString() {
    return "[Event headers = " + headers + " ]";
  }

}
