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
package com.cloudera.cdk.morphline.parser;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.tika.mime.MediaType;
import org.slf4j.Logger;

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.Configs;
import com.cloudera.cdk.morphline.api.Fields;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.MorphlineRuntimeException;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.AbstractCommand;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

/**
 * Base class for convenient implementation of morphline parsers.
 */
public abstract class AbstractParser extends AbstractCommand {

  // TODO: replace tika MediaType with guava MediaType to avoid needing to depend on tika-core
  private Set<MediaType> supportedMimeTypes = null;

  public static final String SUPPORTED_MIME_TYPES = "supportedMimeTypes";
  
  public AbstractParser(Config config, Command parent, Command child, MorphlineContext context) {
    super(config, parent, child, context);      
    if (config.hasPath(SUPPORTED_MIME_TYPES)) {
      List<String> mimeTypes = Configs.getStringList(config, SUPPORTED_MIME_TYPES, Collections.EMPTY_LIST);
      for (String streamMediaType : mimeTypes) {
        addSupportedMimeType(parseMediaType(streamMediaType).getBaseType());
      }
    }
  }

  protected void addSupportedMimeType(MediaType mediaType) {
    if (supportedMimeTypes == null) {
      supportedMimeTypes = new HashSet();
    }
    supportedMimeTypes.add(mediaType.getBaseType());
  }

  @Override
  public boolean process(Record record) {
    if (!hasAtLeastOneAttachment(record)) {
      return false;
    }

    // TODO: make field for stream configurable
    String streamMediaType = (String) record.getFirstValue(Fields.ATTACHMENT_MIME_TYPE);
    if (!isMimeTypeSupported(streamMediaType, record)) {
      return false;
    }

    InputStream stream = createAttachmentInputStream(record);

    return process(record, stream);
  }
  
  protected abstract boolean process(Record record, InputStream stream);

  private boolean isMimeTypeSupported(String mediaTypeStr, Record record) {
    if (supportedMimeTypes == null) {
      return true;
    }
    if (!hasAtLeastOneMimeType(record)) {
      return false;
    }
    MediaType mediaType = parseMediaType(mediaTypeStr).getBaseType();
    if (supportedMimeTypes.contains(mediaType)) {
      return true; // fast path
    }
    // wildcard matching
    for (MediaType rangePattern : supportedMimeTypes) {      
      if (isMediaTypeMatch(mediaType, rangePattern)) {
        return true;
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("No supported MIME type found for " + Fields.ATTACHMENT_MIME_TYPE + "=" + mediaTypeStr);
    }
    return false;
  }

  protected MediaType parseMediaType(String mediaTypeStr) {
    return MediaType.parse(mediaTypeStr.trim().toLowerCase(Locale.ROOT));
  };
      
  /** Returns true if mediaType falls withing the given range (pattern), false otherwise */
  protected boolean isMediaTypeMatch(MediaType mediaType, MediaType rangePattern) {
    String WILDCARD = "*";
    String rangePatternType = rangePattern.getType();
    String rangePatternSubtype = rangePattern.getSubtype();
    return (rangePatternType.equals(WILDCARD) || rangePatternType.equals(mediaType.getType()))
        && (rangePatternSubtype.equals(WILDCARD) || rangePatternSubtype.equals(mediaType.getSubtype()));
  }

  protected String detectCharset(Record record, String charset) {
    if (charset != null) {
      return charset;
    }
    List charsets = record.getFields().get(Fields.ATTACHMENT_CHARSET);
    if (charsets.size() == 0) {
      // TODO try autodetection (AutoDetectReader)
      throw new MorphlineRuntimeException("Missing charset for record: " + record); 
    }
    String charsetName = (String) charsets.get(0);        
    return charsetName;
  }

  protected boolean hasAtLeastOneAttachment(Record record) {
    List attachments = record.getFields().get(Fields.ATTACHMENT_BODY);
    if (attachments.size() == 0) {
      LOG.debug("Command failed because of missing attachment for record: {}", record);
      return false;
    }

    Preconditions.checkNotNull(attachments.get(0));    
    return true;
  }
  
  protected boolean hasAtLeastOneMimeType(Record record) {
    List mimeTypes = record.getFields().get(Fields.ATTACHMENT_MIME_TYPE);
    if (mimeTypes.size() == 0) {
      LOG.debug("Command failed because of missing MIME type for record: {}", record);
      return false;
    }
  
    return true;
  }

  protected InputStream createAttachmentInputStream(Record record) {
    Object body = record.getFirstValue(Fields.ATTACHMENT_BODY);
    Preconditions.checkNotNull(body);
    if (body instanceof byte[]) {
      return new ByteArrayInputStream((byte[]) body);
    } else {
      return (InputStream) body;
    }
  }

  protected void removeAttachments(Record outputRecord) {
    outputRecord.removeAll(Fields.ATTACHMENT_BODY);
    outputRecord.removeAll(Fields.ATTACHMENT_MIME_TYPE);
    outputRecord.removeAll(Fields.ATTACHMENT_CHARSET);
    outputRecord.removeAll(Fields.ATTACHMENT_NAME);
  }

//public static XMediaType toGuavaMediaType(MediaType tika) {
//return XMediaType.create(tika.getType(), tika.getSubtype()).withParameters(Multimaps.forMap(tika.getParameters()));
//}
//
//public static List<XMediaType> toGuavaMediaType(Iterable<MediaType> tikaCollection) {
//List<XMediaType> list = new ArrayList();
//for (MediaType tika : tikaCollection) {
//  list.add(toGuavaMediaType(tika));
//}
//return list;
//}

}

