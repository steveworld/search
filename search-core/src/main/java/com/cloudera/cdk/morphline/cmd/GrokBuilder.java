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
package com.cloudera.cdk.morphline.cmd;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.CommandBuilder;
import com.cloudera.cdk.morphline.api.Configs;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.MorphlineParsingException;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.AbstractCommand;
import com.google.code.regexp.Matcher;
import com.google.code.regexp.Pattern;
import com.google.common.base.Joiner;
import com.google.common.io.CharStreams;
import com.google.common.io.Closeables;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * TODO
 */
public final class GrokBuilder implements CommandBuilder {

  @Override
  public String getName() {
    return "grok";
  }
  
  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    try {
      return new Grok(config, parent, child, context);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class Grok extends AbstractCommand {

    private final Map<String, String> dictionary = new HashMap();
    private final Map<String, Pattern> regexes = new HashMap();
    
    public Grok(Config config, Command parent, Command child, MorphlineContext context) throws IOException {
      super(config, parent, child, context);
      
      for (String dictionaryFile : Configs.getStringList(config, "dictionaryFiles", Collections.EMPTY_LIST)) {
        loadDictionaryFile(new File(dictionaryFile));
      }
      String dictionaryString = Configs.getString(config, "dictionaryString", "");
      loadDictionary(new StringReader(dictionaryString));
      resolveDictionaryExpressions();
      
      Config regexConfig = Configs.getConfig(config, "regexes", ConfigFactory.empty());
      for (Map.Entry<String, Object> entry : regexConfig.root().unwrapped().entrySet()) {
        String expr = entry.getValue().toString();
        //LOG.debug("expr before: {}", expr);
        expr = resolveExpression(expr);     
        //LOG.debug("expr after : {}", expr);
        
        // TODO extract and replace conversions (?<queue_field:int>foo)
        
        Pattern pattern = Pattern.compile(expr);
        regexes.put(entry.getKey(), pattern);
      }
    }
    
    private void loadDictionaryFile(File fileOrDir) throws IOException {
      if (fileOrDir.isDirectory()) {
        File[] files = fileOrDir.listFiles();
        for (File file : files) {
          loadDictionaryFile(file);
        }
      } else {
        Reader reader = new InputStreamReader(new FileInputStream(fileOrDir), "UTF-8");
        try {
          loadDictionary(reader);
        } finally {
          Closeables.closeQuietly(reader);
        }
      }      
    }
    
    private void loadDictionary(Reader reader) throws IOException {
      for (String line : CharStreams.readLines(reader)) {
        line = line.trim();
        if (line.length() == 0) {
          continue; // ignore empty lines
        }
        if (line.startsWith("#")) {
          continue; // ignore comment lines
        }
        int i = line.indexOf(" ");
        if (i < 0) {
          throw new MorphlineParsingException("Dictionary entry line must contain a space to separate name and value: " + line, getConfig());
        }
        if (i == 0) {
          throw new MorphlineParsingException("Dictionary entry line must contain a name: " + line, getConfig());
        }
        String name = line.substring(0, i);
        String value = line.substring(i + 1, line.length()).trim();
        if (value.length() == 0) {
          throw new MorphlineParsingException("Dictionary entry line must contain a value: " + line, getConfig());
        }
        dictionary.put(name, value);
      }      
    }
    
    private void resolveDictionaryExpressions() {
      boolean wasModified = true;
      while (wasModified) {
        wasModified = false;
        for (Map.Entry<String, String> entry : dictionary.entrySet()) {
          String expr = entry.getValue();
          String resolvedExpr = resolveExpression(expr);        
          wasModified = (expr != resolvedExpr);
          if (wasModified) {
            entry.setValue(resolvedExpr);
            break;
          }
        }
      }
      LOG.debug("dictionary: {}", Joiner.on("\n").join(new TreeMap(dictionary).entrySet()));
      for (Map.Entry<String, String> entry : dictionary.entrySet()) {
        Pattern.compile(entry.getValue()); // validate syntax
      }
    }

    private String resolveExpression(String expr) {
      String PATTERN_START = "%{";
      String PATTERN_END= "}";
      char SEPARATOR = ':';
      while (true) {
        int i = expr.indexOf(PATTERN_START);
        if (i < 0) {
          break;
        }     
        int j = expr.indexOf(PATTERN_END, i + PATTERN_START.length());
        if (j < 0) {
          break;
        }     
        String grokPattern = expr.substring(i + PATTERN_START.length(),  j);
        //LOG.debug("grokPattern=" + grokPattern + ", entryValue=" + entryValue);
        int p = grokPattern.indexOf(SEPARATOR);
        String patternName = grokPattern;
        String groupName = null;
        String conversion = null; // FIXME
        if (p >= 0) {
          patternName = grokPattern.substring(0, p);
          groupName = grokPattern.substring(p+1, grokPattern.length());
          int q = groupName.indexOf(SEPARATOR);
          if (q >= 0) {
            conversion = groupName.substring(q+1, groupName.length());
            groupName = groupName.substring(0, q);
          }
        }
        //LOG.debug("patternName=" + patternName + ", groupName=" + groupName + ", conversion=" + conversion);
        String refValue = dictionary.get(patternName);
        if (refValue == null) {
          throw new MorphlineParsingException("Missing value for name: " + patternName, getConfig());
        }
        if (refValue.contains(PATTERN_START)) {
          break; // not a literal value; defer resolution until next iteration
        }
        String replacement = refValue;
        if (groupName != null) { // named capturing group
          replacement = "(?<" + groupName + ">" + replacement + ")";
        }
        expr = new StringBuilder(expr).replace(i, j + PATTERN_END.length(), replacement).toString();
      }
      return expr;
    }
        
    @Override
    public boolean process(Record record) {
      Record outputRecord = record.copy();
      for (Map.Entry<String, Pattern> regexEntry : regexes.entrySet()) {
        Pattern pattern = regexEntry.getValue();
        int numMatches = 0;
        List values = record.getFields().get(regexEntry.getKey());
        for (Object value : values) {
          String strValue = value.toString();
          Matcher matcher = pattern.matcher(strValue);
          if (matcher.matches()) {
            numMatches++;
            for (String groupName : pattern.groupNames()) {
              outputRecord.getFields().put(groupName, matcher.group(groupName));
            }
          }
        }
        if (numMatches == 0) {
          return false;
        }
      }
      return super.process(outputRecord);
    }
    
  }
  
}
