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

import java.io.IOException;
import java.util.List;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.ResettableInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * A deserializer that reads a whole file BLOB per event.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class BlobDeserializer implements EventDeserializer {

  private ResettableInputStream in;
  private final int maxFileLength;
  private volatile boolean isOpen;

  public static final String MAXFILE_KEY = "maxFileLength";
  public static final int MAXFILE_DFLT = 64 * 1024 * 1024;

  private static final int DEFAULT_BUFFER_SIZE = 1024 * 8;
  private static final Logger LOGGER = LoggerFactory.getLogger(BlobDeserializer.class);
      
  protected BlobDeserializer(Context context, ResettableInputStream in) {
    this.in = in;
    this.maxFileLength = context.getInteger(MAXFILE_KEY, MAXFILE_DFLT);
    this.isOpen = true;
  }

  /**
   * Reads a blob from a file and returns an event
   * @return Event containing parsed line
   * @throws IOException
   */
  @SuppressWarnings("resource")
  @Override
  public Event readEvent() throws IOException {
    ensureOpen();
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    byte[] buf = new byte[DEFAULT_BUFFER_SIZE];
    int totalCount = 0;
    int n = 0;
    while ((n = in.read(buf, 0, Math.min(buf.length, maxFileLength - totalCount))) != -1) {
      output.write(buf, 0, n);
      totalCount += n;
      if (totalCount >= maxFileLength) {
        LOGGER.warn("File length exceeds max ({}), truncating event!", maxFileLength);
        break;
      }
    }
    byte[] fileBytes = output.toByteArray();
    if (n == -1 && totalCount == 0) {
      return null;
    } else {
      return EventBuilder.withBody(fileBytes);
    }
  }
  
  /**
   * Batch blob read
   * @param numEvents Maximum number of events to return.
   * @return List of events containing read blobs
   * @throws IOException
   */
  @Override
  public List<Event> readEvents(int numEvents) throws IOException {
    ensureOpen();
    List<Event> events = Lists.newLinkedList();
    for (int i = 0; i < numEvents; i++) {
      Event event = readEvent();
      if (event != null) {
        events.add(event);
      } else {
        break;
      }
    }
    return events;
  }

  @Override
  public void mark() throws IOException {
    ensureOpen();
    in.mark();
  }

  @Override
  public void reset() throws IOException {
    ensureOpen();
    in.reset();
  }

  @Override
  public void close() throws IOException {
    if (isOpen) {
      reset();
      in.close();
      isOpen = false;
    }
  }

  private void ensureOpen() {
    if (!isOpen) {
      throw new IllegalStateException("Serializer has been closed");
    }
  }

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  /** Builder implementations MUST have a public no-arg constructor */
  public static class Builder implements EventDeserializer.Builder {

    @Override
    public BlobDeserializer build(Context context, ResettableInputStream in) {      
      return new BlobDeserializer(context, in);
    }

  }

}
