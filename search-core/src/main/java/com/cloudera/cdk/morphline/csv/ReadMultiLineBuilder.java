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
package com.cloudera.cdk.morphline.csv;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.CommandBuilder;
import com.cloudera.cdk.morphline.api.Configs;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.MorphlineRuntimeException;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.tika.AbstractParser;
import com.google.common.io.Closeables;
import com.typesafe.config.Config;

/**
 * Multiline log parser that collapse multiline messages into a single record; supports "pattern",
 * "what" and "negate" config parameters similar to logstash.
 * 
 * For example, this can be used to parse log4j with stack traces. Also see
 * https://gist.github.com/smougenot/3182192 and http://logstash.net/docs/1.1.9/filters/multiline
 * 
 * Example:
 * 
 * <pre>
 * pattern : "(^.+Exception: .+)|(^\\s+at .+)|(^\\s+... \\d+ more)|(^\\s*Caused by:.+)"
 * negate: false
 * previous : true
 * </pre>
 */
public final class ReadMultiLineBuilder implements CommandBuilder {

  @Override
  public String getName() {
    return "readMultiLine";
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new ReadMultiLine(config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class ReadMultiLine extends AbstractParser {

    private final Pattern pattern;
    private final boolean negate;
    private final boolean previous;
    private final String charset;
  
    public ReadMultiLine(Config config, Command parent, Command child, MorphlineContext context) {
      super(config, parent, child, context);
      this.pattern = Pattern.compile(Configs.getString(config, "pattern"));
      this.negate = Configs.getBoolean(config, "negate", false);
      this.previous = Configs.getBoolean(config, "previous");
      this.charset = Configs.getString(config, "charset", null);
    }

    @Override
    protected boolean process(Record inputRecord, InputStream stream) {
      String charsetName = detectCharset(inputRecord, charset);  
      Reader reader = null;
      try {
        reader = new InputStreamReader(stream, charsetName);
        BufferedReader lineReader = new BufferedReader(reader);
        Matcher matcher = pattern.matcher("");
        StringBuilder lines = null;
        String line;
        
        while ((line = lineReader.readLine()) != null) {
          if (lines == null) {
            lines = new StringBuilder(line);
          } else {
            boolean isMatch = matcher.reset(line).matches();
            if (negate) {
              isMatch = !isMatch;
            }
            /*
            not match && previous --> do next
            not match && next     --> do previous
            match && previous     --> do previous
            match && next         --> do next             
            */
            boolean doPrevious = previous;
            if (!isMatch) {
              doPrevious = !doPrevious;
            }
            
            if (doPrevious) { // do previous
              lines.append('\n');
              lines.append(line);
            } else {          // do next
              if (lines.length() > 0 && !flushRecord(inputRecord, lines.toString())) {
                return false;
              }
              lines.setLength(0);
              lines.append(line);              
            }
          }          
        }
        if (lines != null) {
          return flushRecord(inputRecord, lines.toString());
        }
        return true;
      } catch (IOException e) {
        throw new MorphlineRuntimeException(e);
      } finally {
        if (reader != null) {
          Closeables.closeQuietly(reader);
        }
      }
    }

    private boolean flushRecord(Record inputRecord, String lines) {
      Record outputRecord = new Record(inputRecord);
      removeAttachments(outputRecord);
      outputRecord.replaceValues(Record.MESSAGE, lines);
      
      // pass record to next command in chain:
      return getChild().process(outputRecord);
    }
    
  }
}
