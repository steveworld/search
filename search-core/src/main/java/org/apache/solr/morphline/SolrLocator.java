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
import java.net.MalformedURLException;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.schema.IndexSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.cloudera.cdk.morphline.api.Configs;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.MorphlineRuntimeException;
import com.google.common.base.Preconditions;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigUtil;

/**
 * Set of configuration parameters that identify the location and schema of a Solr server or
 * SolrCloud; Based on this information this class can return the schema and a corresponding
 * {@link DocumentLoader}.
 */
public class SolrLocator {
  
  private MorphlineContext context;
  private String collectionName;
  private String zkHost;
  private String solrUrl;
  private String solrHomeDir;
  private int batchSize = 100;
  
  private static final String SOLR_HOME_PROPERTY_NAME = "solr.solr.home";

  private static final Logger LOG = LoggerFactory.getLogger(SolrLocator.class);

  protected SolrLocator(MorphlineContext context) {
    Preconditions.checkNotNull(context);
    this.context = context;
  }

  public SolrLocator(Config config, MorphlineContext context) {
    this(context);
    collectionName = Configs.getString(config, "collection");
    batchSize = Configs.getInt(config, "batchSize", batchSize);
    zkHost = Configs.getString(config, "zkHost", null);
    solrHomeDir = Configs.getString(config, "solrHomeDir", null);
    solrUrl = Configs.getString(config, "solrUrl", "http://127.0.0.1:8983/solr/" + collectionName);    
    LOG.trace("Constructed solrLocator: {}", this);
  }
  
  public DocumentLoader getLoader() {
    DocumentLoader loader = ((SolrMorphlineContext)context).getDocumentLoader();
    if (loader != null) {
      return loader;
    }
    if (zkHost != null && zkHost.length() > 0) {
      try {
        CloudSolrServer cloudSolrServer = new CloudSolrServer(zkHost);
        cloudSolrServer.setDefaultCollection(collectionName);
        cloudSolrServer.connect();
        return new SolrServerDocumentLoader(cloudSolrServer, batchSize);
      } catch (MalformedURLException e) {
        throw new MorphlineRuntimeException(e);
      }
    } else {
      int solrServerNumThreads = 2;
      int solrServerQueueLength = solrServerNumThreads;
      SolrServer server = new SafeConcurrentUpdateSolrServer(solrUrl, solrServerQueueLength, solrServerNumThreads);
      // SolrServer server = new HttpSolrServer(solrServerUrl);
      // SolrServer server = new ConcurrentUpdateSolrServer(solrServerUrl, solrServerQueueLength, solrServerNumThreads);
      // server.setParser(new XMLResponseParser()); // binary parser is used by default
      return new SolrServerDocumentLoader(server, batchSize);
    }
  }

  public IndexSchema getIndexSchema() {
    String oldSolrHomeDir = null;
    if (solrHomeDir != null && solrHomeDir.length() > 0) {
      oldSolrHomeDir = System.setProperty(SOLR_HOME_PROPERTY_NAME, solrHomeDir);
    }
    try {
      SolrConfig solrConfig = new SolrConfig();
      // SolrConfig solrConfig = new SolrConfig("solrconfig.xml");
      // SolrConfig solrConfig = new
      // SolrConfig("/cloud/apache-solr-4.0.0-BETA/example/solr/collection1",
      // "solrconfig.xml", null);
      // SolrConfig solrConfig = new
      // SolrConfig("/cloud/apache-solr-4.0.0-BETA/example/solr/collection1/conf/solrconfig.xml");

      return new IndexSchema(solrConfig, null, null);
    } catch (ParserConfigurationException e) {
      throw new MorphlineRuntimeException(e);
    } catch (IOException e) {
      throw new MorphlineRuntimeException(e);
    } catch (SAXException e) {
      throw new MorphlineRuntimeException(e);
    } finally { // restore old global state
      if (solrHomeDir != null) {
        if (oldSolrHomeDir == null) {
          System.clearProperty(SOLR_HOME_PROPERTY_NAME);
        } else {
          System.setProperty(SOLR_HOME_PROPERTY_NAME, oldSolrHomeDir);
        }
      }
    }
  }
  
  @Override
  public String toString() {
    return toConfig(null).root().render(ConfigRenderOptions.concise());
  }
  
  public Config toConfig(String key) {
    String json = "";
    if (key != null) {
      json = toJson(key) + " : ";
    }
    json +=  
        "{" +
        " collection : " + toJson(collectionName) + ", " +
        " zkHost : " + toJson(zkHost) + ", " +
        " solrUrl : " + toJson(solrUrl) + ", " +
        " solrHomeDir : " + toJson(solrHomeDir) + ", " +
        " batchSize : " + toJson(batchSize) + " " +
        "}";
    return ConfigFactory.parseString(json);
  }
  
  private String toJson(Object key) {
    String str = key == null ? "" : key.toString();
    str = ConfigUtil.quoteString(str);
    return str;
  }

  public String getCollectionName() {
    return this.collectionName;
  }

  public void setCollectionName(String collectionName) {
    this.collectionName = collectionName;
  }

  public String getZkHost() {
    return this.zkHost;
  }

  public void setZkHost(String zkHost) {
    this.zkHost = zkHost;
  }

  public String getSolrHomeDir() {
    return this.solrHomeDir;
  }

  public void setSolrHomeDir(String solrHomeDir) {
    this.solrHomeDir = solrHomeDir;
  }

  public String getServerUrl() {
    return this.solrUrl;
  }

  public void setServerUrl(String solrUrl) {
    this.solrUrl = solrUrl;
  }

  public int getBatchSize() {
    return this.batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }
  
}
