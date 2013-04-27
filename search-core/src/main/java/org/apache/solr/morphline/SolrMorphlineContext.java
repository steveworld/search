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
import com.google.common.base.Preconditions;

/**
 * A context that is specific to Solr, in particular, includes the Solr schema of a Solr collection.
 */
public class SolrMorphlineContext extends MorphlineContext {

  private IndexSchema schema;
  
  /** For public access use {@link Builder#build()} instead */  
  protected SolrMorphlineContext() {}
  
  public IndexSchema getIndexSchema() {
    assert schema != null;
    return schema;
  }

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  /**
   * Helper to construct a {@link SolrMorphlineContext} instance.
   */
  public static class Builder extends MorphlineContext.Builder {
        
    private IndexSchema schema;
    
    public Builder setIndexSchema(IndexSchema schema) {
      Preconditions.checkNotNull(schema);
      this.schema = schema;
      return this;
    }    

    @Override
    public SolrMorphlineContext build() {
      Preconditions.checkNotNull(schema, "build() requires a prior call to setIndexSchema()");
      ((SolrMorphlineContext)context).schema = schema;
      return (SolrMorphlineContext) super.build();
    }

    @Override
    protected SolrMorphlineContext create() {
      return new SolrMorphlineContext();
    }
    
  }
 
}
