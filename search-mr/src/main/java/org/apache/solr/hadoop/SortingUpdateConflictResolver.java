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
package org.apache.solr.hadoop;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

/**
 * UpdateConflictResolver implementation that orders colliding updates ascending
 * from least recent to most recent (partial) update, based on a configurable
 * numeric Solr field, which defaults to the file_last_modified timestamp.
 */
public class SortingUpdateConflictResolver implements UpdateConflictResolver, Configurable {

  private Configuration conf;
  private String orderByFieldName = ORDER_BY_FIELD_NAME_DEFAULT;
  
  public static final String ORDER_BY_FIELD_NAME_KEY = SortingUpdateConflictResolver.class.getName() + ".orderByFieldName";
  public static final String ORDER_BY_FIELD_NAME_DEFAULT = "file_last_modified";

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
  public Iterator<SolrInputDocument> orderUpdates(Text uniqueKey, Iterator<SolrInputDocument> collidingUpdates) {    
    return sort(collidingUpdates, getOrderByFieldName(), new TimeStampComparator());
  }

  protected Iterator<SolrInputDocument> sort(Iterator<SolrInputDocument> collidingUpdates, final String fieldName, final Comparator c) {
    // TODO: use an external merge sort in the pathological case where there are a huge amount of collisions
    List<SolrInputDocument> sortedUpdates = new ArrayList(1); 
    while (collidingUpdates.hasNext()) {
      sortedUpdates.add(collidingUpdates.next());
    }
    if (sortedUpdates.size() > 1) { // conflicts are rare
      Collections.sort(sortedUpdates, new Comparator<SolrInputDocument>() {
        @Override
        public int compare(SolrInputDocument doc1, SolrInputDocument doc2) {
          SolrInputField f1 = doc1.getField(fieldName);
          SolrInputField f2 = doc2.getField(fieldName);
          if (f1 == f2) {
            return 0;
          } else if (f1 == null) {
            return -1;
          } else if (f2 == null) {
            return 1;
          }
          
          Object v1 = f1.getFirstValue();
          Object v2 = f2.getFirstValue();          
          return c.compare(v1, v2);
        }
      });
    }
    return sortedUpdates.iterator();
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  public static final class TimeStampComparator implements Comparator {

    @Override
    public int compare(Object v1, Object v2) {
      if (v1 == v2) {
        return 0;
      } else if (v1 == null) {
        return -1;
      } else if (v2 == null) {
        return 1;
      }
      long t1 = getLong(v1);
      long t2 = getLong(v2);          
      return (t1 < t2 ? -1 : (t1==t2 ? 0 : 1));
    }
    
    private long getLong(Object v) {
      if (v instanceof Long) {
        return ((Long) v).longValue();
      } else {
        return Long.parseLong(v.toString());
      }
    }      
    
  }

}
