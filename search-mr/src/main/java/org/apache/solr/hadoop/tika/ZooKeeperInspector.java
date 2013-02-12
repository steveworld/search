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
import java.util.List;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.hadoop.tika.TikaIndexerTool.Options;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperInspector {
  private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperInspector.class);
  
  public void extractShardCountAndSolrUrlsFromZk(Options options)
      throws KeeperException, InterruptedException {
    // TODO: assert if one is set, bother are set (collectin, zkServerAddress
    // also check that --solrurl is not being used
    
    SolrZkClient zkClient = null;
    try {
      zkClient = new SolrZkClient(options.zkHost, 15000);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not connect to ZooKeeper", e);
    }
    
    try {
      ZkStateReader zkStateReader = new ZkStateReader(zkClient);
      try {
        zkStateReader.createClusterStateWatchersAndUpdate();
      } catch (Exception e) {
        throw new IllegalArgumentException("Did not find expected information for SolrCloud in ZooKeeper", e);
      }
      
      DocCollection docCollection;
      try {
        docCollection = zkStateReader.getClusterState()
          .getCollection(options.collection);
      } catch (SolrException e) {
        throw new IllegalArgumentException("Could not find collection in ZooKeeper: "
            + options.collection, e);
      }

      Collection<Slice> slices = docCollection.getSlices();
      options.shards = slices.size();
      List<String> solrUrls = new ArrayList<String>(slices.size());
      for (Slice slice : slices) {
        if (slice.getLeader() == null) {
          throw new IllegalArgumentException("It looks like not all of your shards are registered in ZooKeeper yet");
        }
        ZkCoreNodeProps props = new ZkCoreNodeProps(slice.getLeader());
        solrUrls.add(props.getCoreUrl());
      }
      options.shardUrls = solrUrls;
    } finally {
      if (zkClient != null) {
        zkClient.close();
      }
    }
  }
  
  public Exception verifyZkHost(String zkHost, String collection) {
    SolrZkClient client = null;
    try {
      try {
        client = new SolrZkClient(zkHost, 15000);
      } catch (Exception e) {
        return new IllegalArgumentException("Could not connect to ZooKeeper at " + zkHost, e);
      }
      
      ZkStateReader zkStateReader = new ZkStateReader(client);
      try {
        zkStateReader.createClusterStateWatchersAndUpdate();
      } catch (Exception e) {
        return new IllegalArgumentException("Did not find expected information for SolrCloud in ZooKeeper - perhaps you meant to add a chroot to the address?", e);
      }
      
      try {
        zkStateReader.getClusterState().getCollection(collection);
      } catch (SolrException e) {
        return new IllegalArgumentException("Could not find collection in ZooKeeper: "
            + collection, e);
      }
      
      
    } finally {
      if (client != null) {
        client.close();
      }
    }
    
    return null;
  }
  
}
