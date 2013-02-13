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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.apache.solr.tika.MediaTypeDetector;
import org.apache.solr.tika.StreamEvent;
import org.apache.tika.metadata.Metadata;

/**
 * Flume Interceptor that auto-detects and sets a media type aka MIME type on
 * events that are intercepted. This kind of packet sniffing can be used for
 * content based routing in a Flume topology.
 * <p>
 * Type detection is based on Apache Tika, which considers the file name pattern
 * via the {@link Metadata#RESOURCE_NAME_KEY} and {@link Metadata#CONTENT_TYPE}
 * input event headers, as well as magic byte patterns at the beginning of the
 * event body. The type detection and mapping is customizable via the
 * tika-mimetypes.xml and custom-mimetypes.xml and tika-config.xml config files,
 * and can be specified via the "tika.config" context parameter.
 * <p>
 * By default the output event header is named
 * {@link MediaTypeInterceptor#DEFAULT_EVENT_HEADER_NAME}.
 * <p>
 * For background see http://en.wikipedia.org/wiki/Internet_media_type
 */
public class MediaTypeInterceptor implements Interceptor {

  private String headerName;
  private boolean preserveExisting;
  private boolean includeHeaders;
  private MediaTypeDetector detector;

  public static final String HEADER_NAME = "headerName";
  public static final String PRESERVE_EXISTING_NAME = "preserveExisting";
  public static final String INCLUDE_HEADER_NAME = "includeHeaders";
  public static final String TIKA_CONFIG_LOCATION = "tika.config"; // ExtractingRequestHandler.CONFIG_LOCATION

  public static final String DEFAULT_EVENT_HEADER_NAME = MediaTypeDetector.DEFAULT_EVENT_HEADER_NAME;

  protected MediaTypeInterceptor(Context context) {
    headerName = context.getString(HEADER_NAME, DEFAULT_EVENT_HEADER_NAME);
    preserveExisting = context.getBoolean(PRESERVE_EXISTING_NAME, true);
    includeHeaders = context.getBoolean(INCLUDE_HEADER_NAME, true);

    String tikaConfigFilePath = context.getString(TIKA_CONFIG_LOCATION);
    detector = new MediaTypeDetector(tikaConfigFilePath);
  }

  @Override
  public void initialize() {
  }

  protected MediaTypeDetector getDetector() {
    return detector;
  }

  /**
   * Detects the content type of the given input event. Returns
   * <code>application/octet-stream</code> if the type of the event can not be
   * detected.
   * <p>
   * It is legal for the event headers to be empty. It is legal for the event
   * body to be <code>null</code> or an empty array. If the event body is not
   * <code>null</code> the detector may read bytes from the start of the body
   * stream to help in type detection.
   * 
   * @return detected media type, or <code>application/octet-stream</code>
   */
  protected String getMediaType(Event event) {
    InputStream in = event.getBody() == null ? null : new ByteArrayInputStream(event.getBody());
    return getDetector().getMediaType(new StreamEvent(in, event.getHeaders()), getMetadata(event));
  }

  protected Metadata getMetadata(Event event) {
    return getDetector().getMetadata(event.getHeaders(), includeHeaders);
  }

  protected boolean isMatch(Event event) {
    return true;
  }

  @Override
  public Event intercept(Event event) {
    Map<String, String> headers = event.getHeaders();
    if (preserveExisting && headers.containsKey(headerName)) {
      ; // we must preserve the existing id
    } else if (isMatch(event)) {
      headers.put(headerName, getMediaType(event));
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

    public Builder() {
    }

    @Override
    public MediaTypeInterceptor build() {
      return new MediaTypeInterceptor(context);
    }

    @Override
    public void configure(Context context) {
      this.context = context;
    }

  }

}
