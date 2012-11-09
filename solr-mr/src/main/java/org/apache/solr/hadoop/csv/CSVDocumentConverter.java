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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.hadoop.SolrDocumentConverter;

/**
 * Simple converter of MapWritable to SolrInputDocument.
 */
public class CSVDocumentConverter extends SolrDocumentConverter<Text, MapWritable> {
  private static final Log LOG = LogFactory.getLog(CSVDocumentConverter.class);

  /**
   * Convert key and value to a SolrInputDocument.
   * @param key considered document id, and stored in "id" field
   * @param value a map of key/value pairs. A simple convention is
   * followed for field naming to type mapping, which is the same
   * as used in default Solr schema.
   */
  public Collection<SolrInputDocument> convert(Text key, MapWritable value) {
    SolrInputDocument doc = new SolrInputDocument();
    ArrayList<SolrInputDocument> list = new ArrayList<SolrInputDocument>();
    doc.addField("id", key.toString());
    for (Entry<Writable, Writable> e : value.entrySet()) {
      String fieldName = e.getKey().toString();
      String fieldValue = e.getValue().toString();
      if (fieldName.endsWith("_dt")) {
        fieldValue = fixDateFormat(fieldValue);
      } else if (fieldName.endsWith("_i")) {
        fieldValue = fixIntFormat(fieldValue);
      } else if (fieldName.endsWith("_d")) {
        fieldValue = fixDoubleFormat(fieldValue);
      }
      doc.addField(fieldName, fieldValue);
    }
    list.add(doc);
    return list;
  }
  
  SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
  Date now = new Date();
  
  private String fixDateFormat(String in) {
    if (in == null || in.trim().length() == 0) {
      in = sdf.format(now);
    }
    if (in.indexOf('T') == -1) {
      in = in + "T00:00:00Z";
    }
    if (!in.endsWith("Z")) {
      in = in + "Z";
    }
    return in;
  }
  
  private String fixIntFormat(String in) {
    int res = 0;
    if (in == null || in.trim().length() == 0) {
      return "0";
    }
    try {
      res = Integer.parseInt(in);
    } catch (Exception e) {
      LOG.warn("Invalid int field: " + in);
    }
    return String.valueOf(res);
  }
  
  private String fixDoubleFormat(String in) {
    double res = 0.0;
    if (in == null || in.trim().length() == 0) {
      return "0.0";
    }
    try {
      res = Double.parseDouble(in);
    } catch (Exception e) {
      LOG.warn("Invalid double field: " + in);
    }
    return String.valueOf(res);
  }
}
