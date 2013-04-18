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

import java.io.InputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.tika.mime.MediaType;

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.Configs;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.MorphlineRuntimeException;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.AbstractCommand;
import com.typesafe.config.Config;

/**
 * Base class for convenient implementation of morphline parsers.
 */
public abstract class AbstractParser extends AbstractCommand {

  private final Set<MediaType> supportedMimeTypes = new HashSet();

  public static final String SUPPORTED_MIME_TYPES = "supportedMimeTypes";
  public static final String ADDITIONAL_SUPPORTED_MIME_TYPES = "additionalSupportedMimeTypes";
  
  public AbstractParser(Config config, Command parent, Command child, MorphlineContext context) {
    super(config, parent, child, context);      
    if (config.hasPath(SUPPORTED_MIME_TYPES)) {
      List<String> mimeTypes = Configs.getStringList(config, SUPPORTED_MIME_TYPES, Collections.EMPTY_LIST);
      for (String streamMediaType : mimeTypes) {
        supportedMimeTypes.add(MediaType.parse(streamMediaType.trim().toLowerCase(Locale.ROOT)));
      }
    }
    if (config.hasPath(ADDITIONAL_SUPPORTED_MIME_TYPES)) {
      List<String> mimeTypes = Configs.getStringList(config, ADDITIONAL_SUPPORTED_MIME_TYPES, Collections.EMPTY_LIST);
      for (String streamMediaType : mimeTypes) {
        supportedMimeTypes.add(MediaType.parse(streamMediaType.trim().toLowerCase(Locale.ROOT)));
      }
    }
  }

  protected Set<MediaType> getSupportedMimeTypes() {
    return supportedMimeTypes;
  }

  protected abstract boolean process(Record record, InputStream stream);

  @Override
  public boolean process(Record record) {
    if (!Attachments.hasAtLeastOneAttachmentWithMimeType(record, LOG)) {
      return false;
    }

    String streamMediaType = (String) record.getFirstValue(Record.ATTACHMENT_MIME_TYPE);
    if (!isMimeTypeSupported(streamMediaType)) {
      return false;
    }

    InputStream stream = Attachments.createAttachmentInputStream(record);

    return process(record, stream);
  }
  
  protected String detectCharset(Record record, String charset) {
    if (charset != null) {
      return charset;
    }
    List charsets = record.getFields().get(Record.ATTACHMENT_CHARSET);
    if (charsets.size() == 0) {
      // TODO try autodetection (AutoDetectReader)
      throw new MorphlineRuntimeException("Missing charset for record: " + record); 
    }
    String charsetName = (String) charsets.get(0);        
    return charsetName;
  }

  protected void removeAttachments(Record outputRecord) {
    outputRecord.removeAll(Record.ATTACHMENT_BODY);
    outputRecord.removeAll(Record.ATTACHMENT_MIME_TYPE);
    outputRecord.removeAll(Record.ATTACHMENT_CHARSET);
    outputRecord.removeAll(Record.ATTACHMENT_NAME);
  }

  // TODO: implement wildcard matching
  private boolean isMimeTypeSupported(String mimeTypeStr) {
    MediaType mediaType = MediaType.parse(mimeTypeStr.trim().toLowerCase(Locale.ROOT));
    if (supportedMimeTypes.contains(mediaType)) {
      return true;
    }
    if (mediaType.hasParameters() && supportedMimeTypes.contains(mediaType.getBaseType())) {
      return true;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("No supported MIME type found for " + Record.ATTACHMENT_MIME_TYPE + "=" + mimeTypeStr);
    }
    return false;
  }

}

