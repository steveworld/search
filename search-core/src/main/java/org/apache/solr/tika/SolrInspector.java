/*
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

package org.apache.solr.tika;


import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.DateUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.handler.extraction.ExtractingRequestHandler;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.schema.IndexSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

/**
 * Extracts various information from solr configuration.
 *  
 * WARNING: This API is not stable and NOT CONSIDERED PUBLIC. It is subject to change without notice!
 */
public class SolrInspector {
  
  public static final String SOLR_COLLECTION_LIST = "collection";
  public static final String SOLR_CLIENT_HOME = "solr.home";
  public static final String ZK_HOST = "zkHost";
  public static final String SOLR_SERVER_URL = "solr.server.url";
  public static final String SOLR_SERVER_BATCH_SIZE = "solr.server.batchSize";
  public static final String SOLR_SERVER_QUEUE_LENGTH = "solr.server.queueLength";
  public static final String SOLR_SERVER_NUM_THREADS = "solr.server.numThreads";
  public static final String SOLR_HOME_PROPERTY_NAME = "solr.solr.home";

  public static final int DEFAULT_SOLR_SERVER_BATCH_SIZE = 100;
  
  private static final Logger LOGGER = LoggerFactory.getLogger(SolrInspector.class);
  
  public SolrCollection createSolrCollection(Config context) {
    return createSolrCollection(context, null);
  }
  
  public SolrCollection createSolrCollection(Config context, DocumentLoader testServer) {
    /*
     * TODO: need to add an API to solrj that allows fetching IndexSchema from
     * the remote Solr server. Plus move Solr cell params out of solrconfig.xml
     * into a nice HOCON config file. This would allow us to have a single
     * source of truth, simplify and make it unnecessary to parse schema.xml and
     * solrconfig.xml on the client side.
     */
    Map<String, SolrCollection> collections = new LinkedHashMap();
    Config subContext = context.getConfig(SOLR_COLLECTION_LIST);
    Set<String> collectionNames = new LinkedHashSet();
    for (Entry<String, ConfigValue> entry : subContext.entrySet()) {
      String name = entry.getKey();
      collectionNames.add(name.substring(0, name.indexOf('.')));
    }
    if (collectionNames.size() == 0) {
      throw new ConfigurationException("Missing collection specification in configuration: " + context);
    }
    if (collectionNames.size() > 1) {
      throw new ConfigurationException("More than one collection specification in configuration: " + context);
    }

    String collectionName = collectionNames.iterator().next();
    String solrHome = null;
    SolrParams params = new MapSolrParams(new HashMap());
    String zkHost = null;
    String solrServerUrl = "http://127.0.0.1:8983/solr/" + collectionName;
    int solrServerBatchSize = DEFAULT_SOLR_SERVER_BATCH_SIZE;
    int solrServerNumThreads = 2;
    int solrServerQueueLength = solrServerNumThreads;
    Collection<String> dateFormats = DateUtil.DEFAULT_DATE_FORMATS;
    IndexSchema schema;
    for (Map.Entry<String, ConfigValue> entry : subContext.entrySet()) {
      if (entry.getKey().equals(collectionName + "." + SOLR_CLIENT_HOME)) {
        solrHome = entry.getValue().unwrapped().toString();
        assert solrHome != null;
      } else if (entry.getKey().equals(collectionName + "." + ZK_HOST)) {
        zkHost = entry.getValue().unwrapped().toString();
        assert zkHost != null;
      } else if (entry.getKey().equals(collectionName + "." + SOLR_SERVER_URL)) {
        solrServerUrl = entry.getValue().unwrapped().toString();
        assert solrServerUrl != null;
      } else if (entry.getKey().equals(collectionName + "." + SOLR_SERVER_BATCH_SIZE)) {
        solrServerBatchSize = Integer.parseInt(entry.getValue().unwrapped().toString());
      }
    }

    LOGGER.debug("solrHome: {}", solrHome);
    LOGGER.debug("zkHost: {}", zkHost);
    LOGGER.debug("solrServerUrl: {}", solrServerUrl);

    String oldSolrHome = null;
    if (solrHome != null) {
      oldSolrHome = System.setProperty(SOLR_HOME_PROPERTY_NAME, solrHome);
    }
    try {
      SolrConfig solrConfig = new SolrConfig();
      // SolrConfig solrConfig = new SolrConfig("solrconfig.xml");
      // SolrConfig solrConfig = new
      // SolrConfig("/cloud/apache-solr-4.0.0-BETA/example/solr/collection1",
      // "solrconfig.xml", null);
      // SolrConfig solrConfig = new
      // SolrConfig("/cloud/apache-solr-4.0.0-BETA/example/solr/collection1/conf/solrconfig.xml");

      schema = new IndexSchema(solrConfig, null, null);
      // schema = new IndexSchema(solrConfig, "schema.xml", null);
      // schema = new IndexSchema(solrConfig,
      // "/cloud/apache-solr-4.0.0-BETA/example/solr/collection1/conf/schema.xml",
      // null);

      for (PluginInfo pluginInfo : solrConfig.getPluginInfos(SolrRequestHandler.class.getName())) {
        if ("/update/extract".equals(pluginInfo.name)) {
          NamedList initArgs = pluginInfo.initArgs;

          // Copied from StandardRequestHandler
          if (initArgs != null) {
            Object o = initArgs.get("defaults");
            if (o != null && o instanceof NamedList) {
              SolrParams defaults = SolrParams.toSolrParams((NamedList) o);
              params = defaults;
            }
            o = initArgs.get("appends");
            if (o != null && o instanceof NamedList) {
              SolrParams appends = SolrParams.toSolrParams((NamedList) o);
            }
            o = initArgs.get("invariants");
            if (o != null && o instanceof NamedList) {
              SolrParams invariants = SolrParams.toSolrParams((NamedList) o);
            }
//              o = initArgs.get("server");
//              if (o != null && o instanceof NamedList) {
//                SolrParams solrServerParams = SolrParams.toSolrParams((NamedList) o);
//                zkConnectString = solrServerParams.get(SOLR_ZK_CONNECT_STRING, zkConnectString);
//                solrServerUrl = solrServerParams.get(SOLR_SERVER_URL, solrServerUrl);
//                solrServerNumThreads = solrServerParams.getInt(SOLR_SERVER_NUM_THREADS, solrServerNumThreads);
//                solrServerQueueLength = solrServerParams.getInt(SOLR_SERVER_QUEUE_LENGTH, solrServerNumThreads);
//              }

            NamedList configDateFormats = (NamedList) initArgs.get(ExtractingRequestHandler.DATE_FORMATS);
            if (configDateFormats != null && configDateFormats.size() > 0) {
              dateFormats = new HashSet<String>();
              Iterator<Map.Entry> it = configDateFormats.iterator();
              while (it.hasNext()) {
                String format = (String) it.next().getValue();
                LOGGER.info("Adding Date Format: {}", format);
                dateFormats.add(format);
              }
            }
          }
          break; // found it
        }
      }
    } catch (ParserConfigurationException e) {
      throw new ConfigurationException(e);
    } catch (IOException e) {
      throw new ConfigurationException(e);
    } catch (SAXException e) {
      throw new ConfigurationException(e);
    } finally { // restore old global state
      if (solrHome != null) {
        if (oldSolrHome == null) {
          System.clearProperty(SOLR_HOME_PROPERTY_NAME);
        } else {
          System.setProperty(SOLR_HOME_PROPERTY_NAME, oldSolrHome);
        }
      }
    }

    SolrCollection solrCollection;
    if (testServer != null) {
      solrCollection = new SolrCollection(collectionName, testServer);
    } else if (zkHost != null) {
      try {
        CloudSolrServer cloudSolrServer = new CloudSolrServer(zkHost);
        cloudSolrServer.setDefaultCollection(collectionName);
        cloudSolrServer.connect();
        solrCollection = new SolrCollection(collectionName, new SolrServerDocumentLoader(cloudSolrServer, solrServerBatchSize));
      } catch (MalformedURLException e) {
        throw new ConfigurationException(e);
      }
    } else {
      // SolrServer server = new HttpSolrServer(solrServerUrl);
      // SolrServer server = new ConcurrentUpdateSolrServer(solrServerUrl,
      // solrServerQueueLength, solrServerNumThreads);
      SolrServer server = new SafeConcurrentUpdateSolrServer(solrServerUrl, solrServerQueueLength,
          solrServerNumThreads);
      solrCollection = new SolrCollection(collectionName, new SolrServerDocumentLoader(server, solrServerBatchSize));
      // server.setParser(new XMLResponseParser()); // binary parser is used
      // by default
    }
    solrCollection.setSchema(schema);
    solrCollection.setSolrParams(params);
    solrCollection.setDateFormats(dateFormats);
    return solrCollection;
  }

}
