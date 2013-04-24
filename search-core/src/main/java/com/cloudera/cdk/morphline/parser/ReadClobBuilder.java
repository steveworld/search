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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.CommandBuilder;
import com.cloudera.cdk.morphline.api.Configs;
import com.cloudera.cdk.morphline.api.Fields;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.MorphlineRuntimeException;
import com.cloudera.cdk.morphline.api.Record;
import com.google.common.io.CharStreams;
import com.google.common.io.Closeables;
import com.typesafe.config.Config;

/**
 * Command that emits one record for the entire input stream of the first attachment, interpreting
 * the stream as a Character Large Object (CLOB).
 */
public final class ReadClobBuilder implements CommandBuilder {

  @Override
  public String getName() {
    return "readClob";
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new ReadClob(config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class ReadClob extends AbstractParser {

    private final String charset;
  
    public ReadClob(Config config, Command parent, Command child, MorphlineContext context) {
      super(config, parent, child, context);
      this.charset = Configs.getString(config, "charset", null);
    }
  
    @Override
    protected boolean process(Record inputRecord, InputStream stream) {
      String charsetName = detectCharset(inputRecord, charset);  
      Reader reader = null;
      try {
        reader = new InputStreamReader(stream, charsetName);
        String clob = CharStreams.toString(reader);
        Record outputRecord = inputRecord.copy();
        removeAttachments(outputRecord);
        outputRecord.replaceValues(Fields.MESSAGE, clob);
          
        // pass record to next command in chain:
        if (!getChild().process(outputRecord)) {
          return false;
        }
        
        return true;
      } catch (IOException e) {
        throw new MorphlineRuntimeException(e);
      } finally {
        Closeables.closeQuietly(reader);
      }
    }
      
  }
}
