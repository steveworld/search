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
package org.apache.solr.morphline;

import org.apache.solr.schema.IndexSchema;

import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.yammer.metrics.core.MetricsRegistry;

/**
 * A context that is specific to Solr, in particular, includes the Solr schema of a Solr collection.
 */
public class SolrMorphlineContext extends MorphlineContext {

  private final IndexSchema schema;
  
  public SolrMorphlineContext(MetricsRegistry metricsRegistry, IndexSchema schema) {
    super(metricsRegistry);
    this.schema = schema;
  }

  public IndexSchema getIndexSchema() {
    return schema;
  }
}
