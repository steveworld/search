/**
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
package org.apache.solr.hadoop.dedup;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.hadoop.HdfsFileFieldNames;
import org.apache.solr.hadoop.Utils;

/**
 * UpdateConflictResolver implementation that ignores all but the most recent
 * document version, based on a configurable numeric Solr field, which defaults
 * to the file_last_modified timestamp.
 */
public class RetainMostRecentUpdateConflictResolver implements UpdateConflictResolver, Configurable {

  private Configuration conf;
  private String orderByFieldName = ORDER_BY_FIELD_NAME_DEFAULT;
  
  public static final String ORDER_BY_FIELD_NAME_KEY = RetainMostRecentUpdateConflictResolver.class.getName() + ".orderByFieldName";
  public static final String ORDER_BY_FIELD_NAME_DEFAULT = HdfsFileFieldNames.FILE_LAST_MODIFIED;

  private static final String COUNTER_GROUP = Utils.getShortClassName(RetainMostRecentUpdateConflictResolver.class);
  private static final String COUNTER_NAME = "Number of ignored duplicates";
  
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    this.orderByFieldName = conf.get(ORDER_BY_FIELD_NAME_KEY, orderByFieldName);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
  
  protected String getOrderByFieldName() {
    return orderByFieldName;
  }
  
  @Override
  public Iterator<SolrInputDocument> orderUpdates(Text uniqueKey, Iterator<SolrInputDocument> collidingUpdates, Context context) {    
    return getMaximum(collidingUpdates, getOrderByFieldName(), new SolrInputDocumentComparator.TimeStampComparator(), context);
  }

  /** Returns the most recent document among the colliding updates */
  protected Iterator<SolrInputDocument> getMaximum(Iterator<SolrInputDocument> collidingUpdates, String fieldName, Comparator child, Context context) {
    SolrInputDocumentComparator comp = new SolrInputDocumentComparator(fieldName, child);
    SolrInputDocument max = null;
    long numDocs = 0;
    while (collidingUpdates.hasNext()) {
      SolrInputDocument next = collidingUpdates.next(); 
      assert next != null;
      if (max == null) {
        max = next;
      } else if (comp.compare(next, max) > 0) {
        max = next;
      } 
      numDocs++;
    }
    assert max != null;
    if (numDocs > 1) {
      context.getCounter(COUNTER_GROUP, COUNTER_NAME).increment(numDocs - 1);
    }
    return Collections.singletonList(max).iterator();
  }
    
}
