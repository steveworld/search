/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.solr.hadoop.tika;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringReader;

import net.sourceforge.argparse4j.helper.ASCIITextWidthCounter;
import net.sourceforge.argparse4j.helper.TextHelper;
import net.sourceforge.argparse4j.internal.ArgumentParserImpl;

import org.apache.hadoop.util.ToolRunner;

/**
 * Nicely formats the output of
 * {@link ToolRunner#printGenericCommandUsage(PrintStream) with the same look and feel that argparse4j uses for help text.
 */
class ToolRunnerHelpFormatter {
  
  public static String getGenericCommandUsage() {
    try {
      return getGenericCommandUsage2();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  private static String getGenericCommandUsage2() throws IOException {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    ToolRunner.printGenericCommandUsage(new PrintStream(bout, true, "UTF-8"));
    String msg = new String(bout.toByteArray(), "UTF-8");
    BufferedReader reader = new BufferedReader(new StringReader(msg));    
    StringBuilder result = new StringBuilder();
    String line;
    while ((line = reader.readLine()) != null) {
      if (!line.startsWith("-")) {
        result.append(line + "\n");
      } else {
        line = line.trim();
        int i = line.indexOf("  ");
        if (i < 0) {
          i = line.indexOf('\t');
        }
        if (i < 0) {
          result.append(line + "\n");          
        } else {
          String title = line.substring(0, i).trim();
          String help = line.substring(i, line.length()).trim();
          bout.reset();
          PrintWriter writer = new PrintWriter(new OutputStreamWriter(bout, "UTF-8"), true);
          TextHelper.printHelp(writer, title, help, new ASCIITextWidthCounter(), ArgumentParserImpl.FORMAT_WIDTH);
          String formattedLine = new String(bout.toByteArray(), "UTF-8");
          result.append(formattedLine);          
        }        
      }
    }
    return result.toString();
  }
}

