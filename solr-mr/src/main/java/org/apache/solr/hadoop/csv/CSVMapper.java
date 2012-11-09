/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.hadoop.csv;

import java.io.IOException;
import java.io.StringReader;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.solr.hadoop.SolrMapper;
import org.apache.solr.internal.csv.CSVParser;
import org.apache.solr.internal.csv.CSVStrategy;

public class CSVMapper extends SolrMapper<LongWritable, Text, Text, MapWritable> {
  private static final Log LOG = LogFactory.getLog(CSVMapper.class);
  
  private Text id = new Text();
  private String prefix = null;
  private boolean skipHeader = false;
  private String[] fieldNames = null;
  private CSVStrategy strategy = new CSVStrategy(',', '"',
          CSVStrategy.COMMENTS_DISABLED, '\\', false, false, true, true);

  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    prefix = context.getConfiguration().get("mapred.task.partition") + "-";
    if (context.getConfiguration().getLong("map.input.start", -1) == 0) {
      skipHeader = true;
    }
    String file = ((FileSplit) context.getInputSplit()).getPath().toUri().getPath();
//    if (file == null) {
//      LOG.warn("no input file name");
//      return;
//    }
    LOG.debug("getting headers for: " + file);
    fieldNames = context.getConfiguration().getStrings(CSVIndexer.FIELD_NAMES_KEY + file);
  }

  private static class MyMapWritable extends MapWritable {
    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      for (Map.Entry<Writable, Writable> entry: entrySet()) {
        builder.append(entry.toString());
      }
      return builder.toString();
    }
  }

  MapWritable res = new MyMapWritable();

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    id.set(prefix + key.toString());
    if (key.get() == 0 && skipHeader) { // this is the CSV header line
      return;
    }
    CSVParser parser = new CSVParser(new StringReader(value.toString()), strategy);
    String[] fields = null;
    try {
      fields = parser.getLine();
    } catch (Exception e) {
      LOG.warn("invalid line: '" + value + "': " + e.toString());
      return;
    }
    if (fieldNames != null && fields.length != fieldNames.length) {
      LOG.warn("header.len != line.len: " + value.toString());
      return;
    }
    res.clear();
    for (int i = 0; i < fields.length; i++) {
      if (fieldNames != null) {
        res.put(new Text(fieldNames[i]), new Text(fields[i]));
      } else {
        res.put(new Text("f" + i), new Text(fields[i]));
      }
    }
    context.write(id, res);
  }
}
