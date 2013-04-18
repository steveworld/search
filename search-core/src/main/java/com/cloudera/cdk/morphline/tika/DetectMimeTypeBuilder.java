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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.CommandBuilder;
import com.cloudera.cdk.morphline.api.Configs;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.MorphlineRuntimeException;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.AbstractCommand;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

/**
 * Command that auto-detects the MIME type of the first attachment, if no MIME type is defined yet.
 */
public final class DetectMimeTypeBuilder implements CommandBuilder {

  @Override
  public String getName() {
    return "detectMimeType";
  }
  
  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new DetectMimeType(config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class DetectMimeType extends AbstractCommand {

    private final Detector detector;
    private final boolean includeMetaData;
    private final boolean excludeParameters;
    
    public DetectMimeType(Config config, Command parent, Command child, MorphlineContext context) {
      super(config, parent, child, context);
      this.includeMetaData = Configs.getBoolean(config, "includeMetaData", false);
      this.excludeParameters = Configs.getBoolean(config, "excludeParameters", true);
      List<String> mimeTypeFiles = null;
      if (config.hasPath("mimeTypesFiles")) {
        mimeTypeFiles = Configs.getStringList(config, "mimeTypesFiles");
        throw new UnsupportedOperationException(); // FIXME
      } else {
        try {
          detector = new TikaConfig().getDetector();
        } catch (TikaException e) {
          throw new MorphlineRuntimeException(e);
        } catch (IOException e) {
          throw new MorphlineRuntimeException(e);
        }
      }      
    }
    
    @Override
    public boolean process(Record record) {
      if (record.getFields().get(Record.ATTACHMENT_MIME_TYPE).size() == 0) {
        List attachments = record.getFields().get(Record.ATTACHMENT_BODY);
        if (attachments.size() > 0) {
          Object attachment = attachments.get(0);
          Preconditions.checkNotNull(attachment);
          InputStream stream;
          if (attachment instanceof byte[]) {
            stream = new ByteArrayInputStream((byte[]) attachment);
          } else {
            stream = (InputStream) attachment;
          }
          
          Metadata metadata = new Metadata();
          
          // If you specify the resource name (the filename, roughly) with this
          // parameter, then Tika can use it in guessing the right MIME type
          String resourceName = (String) record.getFirstValue(Record.ATTACHMENT_NAME);
          if (resourceName != null) {
            metadata.add(Metadata.RESOURCE_NAME_KEY, resourceName);
          }

          // Provide stream's charset as hint to Tika for better auto detection
          String charset = (String) record.getFirstValue(Record.ATTACHMENT_CHARSET);
          if (charset != null) {
            metadata.add(Metadata.CONTENT_ENCODING, charset);
          }

          if (includeMetaData) {
            for (Entry<String, Object> entry : record.getFields().entries()) {
              metadata.add(entry.getKey(), entry.getValue().toString());
            }
          }

          String mimeType = getMediaType(stream, metadata, excludeParameters);
          record.replaceValues(Record.ATTACHMENT_MIME_TYPE, mimeType);
        }  
      }
      return super.process(record);
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
    private String getMediaType(InputStream in, Metadata metadata, boolean excludeParameters) {
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

    protected Detector getDetector() {
      return detector;
    }

  }
  
}
