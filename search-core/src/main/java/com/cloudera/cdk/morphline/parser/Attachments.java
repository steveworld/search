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
import java.util.List;

import org.slf4j.Logger;

import com.cloudera.cdk.morphline.api.Record;
import com.google.common.base.Preconditions;

final class Attachments {
  
  public static boolean hasAtLeastOneAttachmentWithMimeType(Record record, Logger LOG) {
    List mimeTypes = record.getFields().get(Record.ATTACHMENT_MIME_TYPE);
    if (mimeTypes.size() == 0) {
      LOG.debug("Command failed because of missing MIME type for record: {}", record);
      return false;
    }
    
    List attachments = record.getFields().get(Record.ATTACHMENT_BODY);
    if (attachments.size() == 0) {
      LOG.debug("Command failed because of missing attachment for record: {}", record);
      return false;
    }

    Preconditions.checkNotNull(attachments.get(0));
    
    return true;
  }
  
 
  public static InputStream createAttachmentInputStream(Record record) {
    Object body = record.getFirstValue(Record.ATTACHMENT_BODY);
    Preconditions.checkNotNull(body);
    if (body instanceof byte[]) {
      return new ByteArrayInputStream((byte[]) body);
    } else {
      return (InputStream) body;
    }
  }

}
