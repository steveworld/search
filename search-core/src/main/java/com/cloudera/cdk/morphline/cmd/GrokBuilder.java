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
import java.util.Set;
import java.util.TreeMap;

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.CommandBuilder;
import com.cloudera.cdk.morphline.api.Configs;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.MorphlineParsingException;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.AbstractCommand;
import com.cloudera.cdk.morphline.shaded.com.google.code.regexp.Matcher;
import com.cloudera.cdk.morphline.shaded.com.google.code.regexp.Pattern;
import com.google.common.base.Joiner;
import com.google.common.io.CharStreams;
import com.google.common.io.Closeables;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * The Grok command uses regex pattern matching to extract structured fields from unstructured log
 * data.
 * <p>
 * It is perfect for syslog logs, apache and other webserver logs, mysql logs, and in general, any
 * log format that is generally written for humans and not computer consumption.
 * <p>
 * A grok command can load zero or more dictionaries. A dictionary is a file or string that contains
 * zero or more REGEX_NAME to REGEX mappings, one per line, separated by space, for example:
 * 
 * <pre>
 * INT (?:[+-]?(?:[0-9]+))
 * HOSTNAME \b(?:[0-9A-Za-z][0-9A-Za-z-]{0,62})(?:\.(?:[0-9A-Za-z][0-9A-Za-z-]{0,62}))*(\.?|\b)
 * </pre>
 * 
 * For example, the regex named "INT" is associated with the pattern <code>[+-]?(?:[0-9]+)</code>
 * and matches strings like "123" and the regex named "HOSTNAME" is associated with the pattern
 * <code>\b(?:[0-9A-Za-z][0-9A-Za-z-]{0,62})(?:\.(?:[0-9A-Za-z][0-9A-Za-z-]{0,62}))*(\.?|\b)</code>
 * and matches strings like "www.google.com".
 * <p>
 * A grok command can contain zero or more grok expressions. Each grok expression refers to a record
 * input field name and can contain zero or more grok patterns. Here is an example grok expression
 * that refers to the input field named "message" and contains two grok patterns:
 * 
 * <pre>
 * regexes : {
 *   message : """\s+%{INT:pid} %{HOSTNAME:my_name_servers}"""
 * }
 * </pre>
 * 
 * The syntax for a grok pattern is %{REGEX_NAME:GROUP_NAME}, for example %{INT:pid} or
 * %{HOSTNAME:my_name_servers}
 * <p>
 * The REGEX_NAME is the name of a regex within a loaded dictionary.
 * <p>
 * The GROUP_NAME is the name of an output field. The content of the named capturing group will be
 * added to this output field of the output record.
 * <p>
 * In addition, grok command supports the following parameters:
 * <p>
 * <ul>
 * <li>extract (boolean): whether or not to add the content of named capturing groups to the output
 * record. Defaults to true.</li>
 * <li>numRequiredMatches (String): indicates the minimum and maximum number of field values that
 * must match a given grok expression, for each input field name. Can be "atLeastOnce" (default) or
 * "once" or "all".</li>
 * <li>findSubstrings (boolean): indicates whether the grok expression must match the entire input
 * field value, or merely a substring within. Defaults to false.</li>
 * </ul>
 */
public final class GrokBuilder implements CommandBuilder {

  /*
   * Uses a shaded version of com.google.code.regexp-0.1.9 to minimize potential dependency issues.
   * See https://github.com/tony19/named-regexp
   */
  
  @Override
  public Set<String> getNames() {
    return Collections.singleton("grok");
  }
  
  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    try {
      return new Grok(config, parent, child, context);
    } catch (IOException e) {
      throw new MorphlineParsingException("Cannot parse", config, e);
    }
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class Grok extends AbstractCommand {

    private final Map<String, String> dictionary = new HashMap();
    private final Map<String, Pattern> regexes = new HashMap();
    private final boolean extract; // whether or not to add the content of named capturing groups to the output record.
    private final NumRequiredMatches numRequiredMatches; // indicates the minimum and maximum number of field values that must match a given grok expression, for each input field name. Can be "atLeastOnce" (default) or "once" or "all".
    private final boolean findSubstrings; // indicates whether the grok expression must match the entire input field value, or merely a substring within. 
    
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
      
      this.extract = Configs.getBoolean(config, "extract", true);
      this.numRequiredMatches = NumRequiredMatches.valueOf(
          Configs.getString(config, "numRequiredMatches", NumRequiredMatches.atLeastOnce.toString()));
      this.findSubstrings = Configs.getBoolean(config, "findSubstrings", false);
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
        String regexName = grokPattern;
        String groupName = null;
        String conversion = null; // FIXME
        if (p >= 0) {
          regexName = grokPattern.substring(0, p);
          groupName = grokPattern.substring(p+1, grokPattern.length());
          int q = groupName.indexOf(SEPARATOR);
          if (q >= 0) {
            conversion = groupName.substring(q+1, groupName.length());
            groupName = groupName.substring(0, q);
          }
        }
        //LOG.debug("patternName=" + patternName + ", groupName=" + groupName + ", conversion=" + conversion);
        String refValue = dictionary.get(regexName);
        if (refValue == null) {
          throw new MorphlineParsingException("Missing value for name: " + regexName, getConfig());
        }
        if (refValue.contains(PATTERN_START)) {
          break; // not a literal value; defer resolution until next iteration
        }
        String replacement = refValue;
        if (groupName != null) { // named capturing group
          replacement = "(?<" + groupName + ">" + refValue + ")";
        }
        expr = new StringBuilder(expr).replace(i, j + PATTERN_END.length(), replacement).toString();
      }
      return expr;
    }
        
    @Override
    public boolean process(Record record) {
      Record outputRecord = extract ? record.copy() : record;
      for (Map.Entry<String, Pattern> regexEntry : regexes.entrySet()) {
        Pattern pattern = regexEntry.getValue();
        List values = record.getFields().get(regexEntry.getKey());
        int minMatches = 1;
        int maxMatches = Integer.MAX_VALUE;
        switch (numRequiredMatches) {
          case once : { 
            maxMatches = 1;
            break;
          }
          case all : { 
            minMatches = values.size();
            break;
          }
          default: {
            break;
          }
        }        
        int numMatches = 0;
        for (Object value : values) {
          String strValue = value.toString();
          Matcher matcher = pattern.matcher(strValue);
          if (!findSubstrings) {
            if (matcher.matches()) {
              numMatches++;
              if (numMatches > maxMatches) {
                return false;
              }
              extract(outputRecord, pattern, matcher);
            }
          } else {
            int previousNumMatches = numMatches;
            while (matcher.find()) {
              if (numMatches == previousNumMatches) {
                numMatches++;
                if (numMatches > maxMatches) {
                  return false;
                }
              }
              extract(outputRecord, pattern, matcher);
            }
          }
        }
        if (numMatches < minMatches) {
          return false;
        }
      }
      return super.process(outputRecord);
    }

    private void extract(Record outputRecord, Pattern pattern, Matcher matcher) {
      if (extract) {
        for (String groupName : pattern.groupNames()) {
          outputRecord.getFields().put(groupName, matcher.group(groupName));
        }
      }
    }

    
    ///////////////////////////////////////////////////////////////////////////////
    // Nested classes:
    ///////////////////////////////////////////////////////////////////////////////
    private static enum NumRequiredMatches {
      atLeastOnce,
      once,
      all     
    }     
  }
  
}
