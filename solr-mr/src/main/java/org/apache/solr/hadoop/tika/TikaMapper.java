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
package org.apache.solr.hadoop.tika;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.solr.hadoop.SolrMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TikaMapper extends SolrMapper<LongWritable, Text, Text, MapWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(TikaMapper.class);
  
  private final Text id = new Text();
  private final MapWritable fields = new MyMapWritable();

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

  /**
   * Extract content from the path specified in the value. Key is useless.
   */
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String uniqueId = "";
    id.set(uniqueId);

    context.write(id, fields);
  }
}
