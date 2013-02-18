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
package org.apache.solr.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.solr.common.SolrInputDocument;

public class SolrReducer extends Reducer<Text, SolrInputDocumentWritable, Text, SolrInputDocumentWritable> {

  private UpdateConflictResolver resolver;
  private HeartBeater heartBeater;
  
  public static final String UPDATE_CONFLICT_RESOLVER = SolrReducer.class.getName() + ".updateConflictResolver";
  
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    SolrRecordWriter.addReducerContext(context);
    Class<? extends UpdateConflictResolver> resolverClass = context.getConfiguration().getClass(UPDATE_CONFLICT_RESOLVER,
        DefaultUpdateConflictResolver.class, UpdateConflictResolver.class);
    
    if (resolverClass != null) {
      resolver = ReflectionUtils.newInstance(resolverClass, context.getConfiguration());
      /*
       * Note that ReflectionUtils.newInstance() above also implicitly calls
       * resolver.configure(context.getConfiguration()) if the resolver
       * implements org.apache.hadoop.conf.Configurable
       */
    }
    heartBeater = new HeartBeater(context);
  }
  
  protected void reduce(Text key, Iterable<SolrInputDocumentWritable> values, Context context) throws IOException, InterruptedException {
    heartBeater.needHeartBeat();
    try {
      if (resolver != null) {        
        values = resolve(key, values);
      }
      super.reduce(key, values, context);
    } finally {
      heartBeater.cancelHeartBeat();
    }
  }

  private Iterable<SolrInputDocumentWritable> resolve(final Text key, final Iterable<SolrInputDocumentWritable> values) {
    return new Iterable<SolrInputDocumentWritable>() {
      @Override
      public Iterator<SolrInputDocumentWritable> iterator() {
        return new WrapIterator(resolver.orderUpdates(key, new UnwrapIterator(values.iterator())));
      }
    };
  }
    
  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    heartBeater.close();
    super.cleanup(context);
  }

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class WrapIterator implements Iterator<SolrInputDocumentWritable> {
    
    private Iterator<SolrInputDocument> iter;

    private WrapIterator(Iterator<SolrInputDocument> iter) {
      this.iter = iter;
    }

    @Override
    public boolean hasNext() {
      return iter.hasNext();
    }

    @Override
    public SolrInputDocumentWritable next() {
      return new SolrInputDocumentWritable(iter.next());
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

  }


  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class UnwrapIterator implements Iterator<SolrInputDocument> {
    
    private Iterator<SolrInputDocumentWritable> iter;

    private UnwrapIterator(Iterator<SolrInputDocumentWritable> iter) {
      this.iter = iter;
    }

    @Override
    public boolean hasNext() {
      return iter.hasNext();
    }

    @Override
    public SolrInputDocument next() {
      return iter.next().getSolrInputDocument();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

}
