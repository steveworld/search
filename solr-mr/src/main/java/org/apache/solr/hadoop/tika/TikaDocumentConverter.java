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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map.Entry;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.hadoop.SolrDocumentConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple converter of MapWritable to SolrInputDocument.
 */
public class TikaDocumentConverter extends SolrDocumentConverter<Text, MapWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(TikaDocumentConverter.class);

  /**
   * Convert key and value to a SolrInputDocument.
   * @param key considered document id, and stored in "id" field
   * @param value a map of key/value pairs.
   */
  public Collection<SolrInputDocument> convert(Text key, MapWritable value) {
    SolrInputDocument doc = new SolrInputDocument();
    ArrayList<SolrInputDocument> list = new ArrayList<SolrInputDocument>();
    doc.addField("id", key.toString());
    for (Entry<Writable, Writable> e : value.entrySet()) {
      String fieldName = e.getKey().toString();
      String fieldValue = e.getValue().toString();
      doc.addField(fieldName, fieldValue);
    }
    list.add(doc);
    return list;
  }
}
