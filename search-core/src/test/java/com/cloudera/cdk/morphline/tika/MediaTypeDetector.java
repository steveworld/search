/**
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.cdk.morphline.tika;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;

import com.cloudera.cdk.morphline.api.MorphlineRuntimeException;

/**
 * Helper that auto-detects and sets a media type aka MIME type on the header of
 * events that are intercepted. This kind of packet sniffing can be used for
 * content based routing in a network topology.
 * <p>
 * Type detection is based on Apache Tika, which considers the file name pattern
 * via the {@link Metadata#RESOURCE_NAME_KEY} and {@link Metadata#CONTENT_TYPE}
 * input event headers, as well as magic byte patterns at the beginning of the
 * event body. The type detection and mapping is customizable via the
 * tika-mimetypes.xml and custom-mimetypes.xml and tika-config.xml config files,
 * and can be specified via the "tika.config" context parameter.
 * <p>
 * By default the output event header is named
 * {@link MediaTypeDetector#DEFAULT_EVENT_HEADER_NAME}.
 * <p>
 * For background see http://en.wikipedia.org/wiki/Internet_media_type
 */
public class MediaTypeDetector {

  private final Detector detector;

  public static final String DEFAULT_EVENT_HEADER_NAME = "stream.type"; // ExtractingParams.STREAM_TYPE;

  public MediaTypeDetector(String tikaConfigFilePath) {
    String oldProperty = null;
    if (tikaConfigFilePath != null) {
      // see TikaConfig() no-arg constructor impl
      oldProperty = System.setProperty("tika.config", tikaConfigFilePath); 
    }

    TikaConfig tikaConfig;
    try {
      tikaConfig = new TikaConfig();
    } catch (TikaException e) {
      throw new MorphlineRuntimeException(e);
    } catch (IOException e) {
      throw new MorphlineRuntimeException(e);
    } finally { // restore old global state
      if (tikaConfigFilePath != null) {
        if (oldProperty == null) {
          System.clearProperty("tika.config");
        } else {
          System.setProperty("tika.config", oldProperty);
        }
      }
    }
    detector = tikaConfig.getDetector();
  }

  /**
   * Detects the content type of the given input event. Returns
   * <code>application/octet-stream</code> if the type of the event can not be
   * detected.
   * <p>
   * It is legal for the event headers or body to be empty. The detector may
   * read bytes from the start of the body stream to help in type detection.
   * 
   * @return detected media type, or <code>application/octet-stream</code>
   */
  public String getMediaType(StreamEvent event, Metadata metadata, boolean excludeParameters) {
    if (metadata == null) {
      throw new NullPointerException();
    }
    InputStream in = event.getBody();
    MediaType mediaType;
    try {
      mediaType = getDetector().detect(in, metadata);
    } catch (IOException e) {
      throw new MorphlineRuntimeException(e);
    }
    String mediaTypeStr = mediaType.toString();
    if (excludeParameters) {
      int i = mediaTypeStr.indexOf(';');
      if (i >= 0) {
        mediaTypeStr = mediaTypeStr.substring(0, i);
      }
    }
    return mediaTypeStr;
  }

  public Metadata getMetadata(Map<String, String> eventHeaders, boolean includeHeaders) {
    Metadata metadata = new Metadata();
    if (includeHeaders) {
      for (Map.Entry<String, String> entry : eventHeaders.entrySet()) {
        metadata.set(entry.getKey(), entry.getValue());
      }
    }
    return metadata;
  }

  protected Detector getDetector() {
    return detector;
  }

}
