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

import org.apache.tika.io.CloseShieldInputStream;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.Record;
import com.google.common.io.Closeables;

/**
 * Adapted from org.apache.tika.extractor.ParsingEmbeddedDocumentExtractor
 */
final class EmbeddedExtractor {

  public boolean parseEmbedded(InputStream stream, Record record, String name, Command child) {
    // Use the delegate parser to parse this entry
    
    TemporaryResources tmp = new TemporaryResources();
    try {
      final TikaInputStream newStream = TikaInputStream.get(new CloseShieldInputStream(stream), tmp);
      if (stream instanceof TikaInputStream) {
        final Object container = ((TikaInputStream) stream).getOpenContainer();
        if (container != null) {
          newStream.setOpenContainer(container);
        }
      }
      record = new Record(record);

      record.replaceValues(Record.ATTACHMENT_BODY, newStream);
      record.removeAll(Record.ATTACHMENT_MIME_TYPE);
      record.removeAll(Record.ATTACHMENT_CHARSET);
      
      record.removeAll(Record.ATTACHMENT_NAME);
      if (name != null && name.length() > 0) {
        record.getFields().put(Record.ATTACHMENT_NAME, name);
      }
      
      return child.process(record);
//    } catch (RuntimeException e) {
//      
//      // THIS IS THE DIFF WRT ParsingEmbeddedDocumentExtractor
//      throw new MorphlineRuntimeException(e);
//      
//        // TODO: can we log a warning somehow?
//        // Could not parse the entry, just skip the content
    } finally {
      Closeables.closeQuietly(tmp);
    }

  }

}

