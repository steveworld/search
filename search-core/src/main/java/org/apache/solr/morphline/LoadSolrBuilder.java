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
package org.apache.solr.morphline;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.CommandBuilder;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.MorphlineRuntimeException;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.AbstractCommand;
import com.cloudera.cdk.morphline.base.Configs;
import com.cloudera.cdk.morphline.base.Notifications;
import com.typesafe.config.Config;

/**
 * A command that loads a record into a SolrServer or MapReduce SolrOutputFormat.
 */
public final class LoadSolrBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("loadSolr");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new LoadSolr(config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class LoadSolr extends AbstractCommand {
    
    private final DocumentLoader loader;

    public LoadSolr(Config config, Command parent, Command child, MorphlineContext context) {
      super(config, parent, child, context);
      Config solrLocatorConfig = Configs.getConfig(config, "solrLocator");
      SolrLocator locator = new SolrLocator(solrLocatorConfig, context);
      LOG.debug("solrLocator: {}", locator);
      this.loader = locator.getLoader();
    }

    @Override
    public void notify(Record notification) {
      for (Object event : Notifications.getLifeCycleEvents(notification)) {
        if (event == Notifications.LifeCycleEvent.BEGIN_TRANSACTION) {
          try {
            loader.beginTransaction();
          } catch (SolrServerException e) {
            throw new MorphlineRuntimeException(e);
          } catch (IOException e) {
            throw new MorphlineRuntimeException(e);
          }
        } else if (event == Notifications.LifeCycleEvent.COMMIT_TRANSACTION) {
          try {
            loader.commitTransaction();
          } catch (SolrServerException e) {
            throw new MorphlineRuntimeException(e);
          } catch (IOException e) {
            throw new MorphlineRuntimeException(e);
          }
        }
        else if (event == Notifications.LifeCycleEvent.ROLLBACK_TRANSACTION) {
          try {
            loader.rollbackTransaction();
          } catch (SolrServerException e) {
            throw new MorphlineRuntimeException(e);
          } catch (IOException e) {
            throw new MorphlineRuntimeException(e);
          }
        }
        else if (event == Notifications.LifeCycleEvent.SHUTDOWN) {
          try {
            loader.shutdown();
          } catch (SolrServerException e) {
            throw new MorphlineRuntimeException(e);
          } catch (IOException e) {
            throw new MorphlineRuntimeException(e);
          }
        }
      }
      super.notify(notification);
    }
    
    @Override
    protected boolean doProcess(Record record) {
      SolrInputDocument doc = convert(record);
      try {
        loader.load(doc);
      } catch (IOException e) {
        throw new MorphlineRuntimeException(e);
      } catch (SolrServerException e) {
        throw new MorphlineRuntimeException(e);
      }
      return super.doProcess(record);
    }
    
    private SolrInputDocument convert(Record record) {
      SolrInputDocument doc = new SolrInputDocument();
      for (Map.Entry<String, Object> entry : record.getFields().entries()) {
        doc.addField(entry.getKey(), entry.getValue()); // TODO optimize?
      }
      return doc;
    }
    
  }
}
