/*
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
package org.apache.solr.crunch;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2;
import org.apache.curator.framework.recipes.locks.Lease;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Rampup lease for rate limiting on morphline init; implemented with a distributed semaphore in ZooKeeper.
 */
final class MorphlineInitRateLimiter {
  
  private final Configuration conf;

  private final CuratorFramework curatorClient;
  
  private final boolean isJobDriver;
  
  static final int UNLIMITED_PARALLEL_MORPHLINE_INITS = Integer.MAX_VALUE;
  
  private static final String JOB_ID_PARAM_NAME = MorphlineInitRateLimiter.class.getName() + ".jobId";

  private static final Logger LOG = LoggerFactory.getLogger(MorphlineInitRateLimiter.class);
  
  public MorphlineInitRateLimiter(Configuration conf, String zkEnsemble, boolean isJobDriver) {
    Preconditions.checkNotNull(conf);
    this.conf = conf;
    this.curatorClient = getCuratorFramwork(zkEnsemble);
    this.curatorClient.start();
    this.isJobDriver = isJobDriver;
    if (isJobDriver) {
      setZkJobDir();
    }
  }

  public void close() {
    try {
      if (isJobDriver) {
        try {
          // garbage collect any ZK state
          String zkJobDir = getZkJobDir();
          if (curatorClient.checkExists().forPath(zkJobDir) != null) {
            curatorClient.delete().deletingChildrenIfNeeded().forPath(zkJobDir);
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    } finally {
      curatorClient.close();
    }
  }
  
  private void setZkJobDir() {
    String jobId = new SimpleDateFormat("yyyyMMdd").format(new Date()) + "-" + UUID.randomUUID().toString();
    conf.set(JOB_ID_PARAM_NAME, jobId);
  }

  private String getZkJobDir() {
    String jobId = conf.get(JOB_ID_PARAM_NAME);
    return "/" + MorphlineInitRateLimiter.class.getName() + "/" + jobId;
  }
  
  private CuratorFramework getCuratorFramwork(String zkHost) {
    Preconditions.checkNotNull(zkHost);
    Preconditions.checkArgument(zkHost.length() > 0);
    int i = zkHost.lastIndexOf('/');
    if (i >= 0) {
      zkHost = zkHost.substring(0, i); // remove trailing /solr base dir
    }
    LOG.debug("zkEnsembleForCurator: {}", zkHost);
    return CuratorFrameworkFactory.newClient(
        zkHost, 
        1000 * conf.getInt("zkClientSessionTimeoutSeconds", 5 * 60), 
        1000 * conf.getInt("zkClientConnectTimeoutSeconds", 5 * 60), 
        new BoundedExponentialBackoffRetry(1000, 10000, 29));
  }
  
  public Lease acquireLease(int parallelMorphlineInits) {
    InterProcessSemaphoreV2 clusterLock = new InterProcessSemaphoreV2(
        curatorClient, 
        getZkJobDir() + "/rampupLock",
        parallelMorphlineInits
        );

    // TODO: Better make this a function of the total number of MR tasks (or Spark tasks)
    int rampupLockTimeoutSeconds = conf.getInt("rampupLockTimeoutSeconds", 60 * 60);
    
    LOG.info("Trying to acquire distributed rampup lease for rate limiting on morphline init ...");
    long acquireStartTime = System.currentTimeMillis();

    Lease acquiredLease;
    try {
      acquiredLease = clusterLock.acquire(rampupLockTimeoutSeconds, TimeUnit.SECONDS);
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to acquire distributed rampup lease for rate limiting on morphline init before timeout of "             
          + rampupLockTimeoutSeconds + "seconds", e);
    }
    
    if (acquiredLease == null) {
      throw new IllegalStateException(
          "Could not acquire the distributed rampup lease for rate limiting on morphline init for " 
          + rampupLockTimeoutSeconds + " seconds");
    }
    
    float secs = (System.currentTimeMillis() - acquireStartTime) / 1000.0f;
    LOG.info("Done acquiring distributed rampup lease for rate limiting on morphline init (took " + secs + " seconds)");

    return acquiredLease;
  }

}
