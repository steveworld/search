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

import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.CommandBuilder;
import com.cloudera.cdk.morphline.api.Fields;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.AbstractCommand;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;

/**
 * TODO
 */
public final class RequireAtMostOneAttachmentBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("requireAtMostOneAttachment");
  }
  
  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new RequireAtMostOneAttachment(config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class RequireAtMostOneAttachment extends AbstractCommand {

    public RequireAtMostOneAttachment(Config config, Command parent, Command child, MorphlineContext context) {
      super(config, parent, child, context);
    }
    
    @Override
    public boolean process(Record record) {
      if (!hasAtMostOneAttachment(record, LOG)) {
        return false;
      }
      return super.process(record);
    }

    private static boolean hasAtMostOneAttachment(Record record, Logger LOG) {
      List mimeTypes = record.getFields().get(Fields.ATTACHMENT_MIME_TYPE);
      if (mimeTypes.size() > 1) {
        LOG.debug("Command failed because the record must not contain more than one MIME type: {}", record);
        return false;
      }
      
      List bodies = record.getFields().get(Fields.ATTACHMENT_BODY);
      if (bodies.size() > 1) {
        LOG.debug("Command failed because the record must not contain more than one attachment: {}", record);
        return false;
      }

      if (bodies.size() > 0) {
        Object body = bodies.get(0);
        Preconditions.checkNotNull(body);
        if (!(body instanceof byte[] || body instanceof InputStream)) {
          LOG.debug("Command failed because the record's attachment must be a byte array or InputStream: {}", record);
          return false;
        }
      }
      
      return true;
    }
   
  }
  
}
