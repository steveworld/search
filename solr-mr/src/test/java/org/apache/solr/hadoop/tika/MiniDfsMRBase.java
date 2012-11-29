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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.TestCase;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MiniDfsMRBase {
  private static final Logger LOG = LoggerFactory.getLogger(MiniDfsMRBase.class);

  @Rule public TestName name = new TestName();

  /**
   * System property to specify the parent directory for the 'sarchtests' directory to be used as base for all test
   * working directories. </p> If this property is not set, the assumed value is '/tmp'.
   */
  public static final String SEARCH_TEST_DIR = "search.test.dir";
  /**
   * System property that specifies the user that test search instance runs as.
   * The value of this property defaults to the "${user.name} system property.
   */
  public static final String TEST_SEARCH_USER_PROP = "search.test.user.search";

  /**
   * System property that specifies the default test user name used by the
   * tests. The default value of this property is <tt>test</tt>.
   */
  public static final String TEST_USER1_PROP = "search.test.user.test";

  /**
   * System property that specifies an auxilliary test user name used by the
   * tests. The default value of this property is <tt>test2</tt>.
   */
  public static final String TEST_USER2_PROP = "search.test.user.test2";

  /**
   * System property that specifies another auxilliary test user name used by
   * the tests. The default value of this property is <tt>test3</tt>.
   */
  public static final String TEST_USER3_PROP = "search.test.user.test3";

  /**
   * System property that specifies the test group used by the tests. The
   * default value of this property is <tt>testg</tt>.
   */
  public static final String TEST_GROUP_PROP = "search.test.group";

  /**
   * System property to specify the Hadoop Job Tracker to use for testing. </p>
   * If this property is not set, the assumed value is 'locahost:9001'.
   */
  public static final String SEARCH_TEST_JOB_TRACKER = "search.test.job.tracker";

  /**
   * System property to specify the Hadoop Name Node to use for testing. </p> If
   * this property is not set, the assumed value is 'locahost:9000'.
   */
  public static final String SEARCH_TEST_NAME_NODE = "search.test.name.node";

  /**
   * System property that specifies the wait time, in seconds, between testcases before
   * triggering a shutdown. The default value is 10 sec.
   */
  public static final String TEST_MINICLUSTER_MONITOR_SHUTDOWN_WAIT = "search.test.minicluster.monitor.shutdown.wait";

  private String testCaseDir;
  
  protected static String getSearchUser() {
    return System.getProperty(TEST_SEARCH_USER_PROP,
        System.getProperty("user.name"));
  }
  protected static String getTestUser() {
    return System.getProperty(TEST_USER1_PROP, "test");
  }
  protected static String getTestGroup() {
    return System.getProperty(TEST_GROUP_PROP, "testg");
  }
  protected static String getTestUser2() {
    return System.getProperty(TEST_USER2_PROP, "test2");
  }
  protected static String getTestUser3() {
    return System.getProperty(TEST_USER3_PROP, "test3");
  }
  protected String getJobTrackerUri() {
      return System.getProperty(SEARCH_TEST_JOB_TRACKER, "localhost:9001");
  }
  protected String getNameNodeUri() {
      return System.getProperty(SEARCH_TEST_NAME_NODE, "hdfs://localhost:9000");
  }
  protected String getTestCaseDir() {
    return testCaseDir;
  }

  protected static MiniDFSCluster dfsCluster = null;
  protected static MiniMRCluster mrCluster = null;

  protected void setup() throws Exception {
    testCaseDir = createTestCaseDir(true);
    setUpEmbeddedHadoop(testCaseDir);
  }

  protected String createTestCaseDir(boolean cleanup)
      throws Exception {
    String testCaseDir = getTestCaseDirInternal();
    LOG.info("Setting testcase work dir[{}]", testCaseDir);
    if (cleanup) {
      delete(new File(testCaseDir));
    }
    File dir = new File(testCaseDir);
    if (!dir.mkdirs()) {
      throw new RuntimeException("Could not create testcase dir[" + testCaseDir
          + "]");
    }
    return testCaseDir;
  }

  protected String getTestCaseDirInternal() {
    File dir = new File(System.getProperty(SEARCH_TEST_DIR, "target/test-data"));
    dir = new File(dir, "searchtests").getAbsoluteFile();
    dir = new File(dir, this.getClass().getName());
    dir = new File(dir, name.getMethodName());
    return dir.getAbsolutePath();
  }

  protected void delete(File file) throws IOException {
    if (file.getAbsolutePath().length() < 5) {
      throw new RuntimeException(
          "path [" + file.getAbsolutePath() + "] is too short, not deleting");
    }
    if (file.exists()) {
      if (file.isDirectory()) {
        File[] children = file.listFiles();
        if (children != null) {
          for (File child : children) {
            delete(child);
          }
        }
      }
      if (!file.delete()) {
        throw new RuntimeException("could not delete path [" + file.getAbsolutePath() + "]");
      }
    }
  }

  protected void setUpEmbeddedHadoop(String testCaseDir) throws Exception {
    if (dfsCluster == null && mrCluster == null) {
      if (System.getProperty("hadoop.log.dir") == null) {
        System.setProperty("hadoop.log.dir", testCaseDir);
      }
      int taskTrackers = 2;
      int dataNodes = 2;
      String searchUser = getSearchUser();
      JobConf conf = new JobConf();
      conf.set("dfs.block.access.token.enable", "false");
      conf.set("dfs.permissions", "true");
      conf.set("hadoop.security.authentication", "simple");

      // Doing this because Hadoop 1.x does not support '*' and
      // Hadoop 0.23.x does not process wildcard if the value is
      // '*,127.0.0.1'
      StringBuilder sb = new StringBuilder();
      sb.append("127.0.0.1,localhost");
      for (InetAddress i : InetAddress.getAllByName(InetAddress.getLocalHost()
          .getHostName())) {
        sb.append(",").append(i.getCanonicalHostName());
      }
      conf.set("hadoop.proxyuser." + searchUser + ".hosts", sb.toString());

      conf.set("hadoop.proxyuser." + searchUser + ".groups", getTestGroup());
      conf.set("mapred.tasktracker.map.tasks.maximum", "4");
      conf.set("mapred.tasktracker.reduce.tasks.maximum", "4");

      String[] userGroups = new String[] { getTestGroup() };
      UserGroupInformation.createUserForTesting(searchUser, userGroups);
      UserGroupInformation.createUserForTesting(getTestUser(), userGroups);
      UserGroupInformation.createUserForTesting(getTestUser2(), userGroups);
      UserGroupInformation.createUserForTesting(getTestUser3(),
          new String[] { "users" });
      conf.set("hadoop.tmp.dir", "target/test-data" + "/minicluster");

      try {
        dfsCluster = new MiniDFSCluster(conf, dataNodes, true, null);
        FileSystem fileSystem = dfsCluster.getFileSystem();
        fileSystem.mkdirs(new Path("target/test-data"));
        fileSystem.mkdirs(new Path("target/test-data" + "/minicluster/mapred"));
        fileSystem.mkdirs(new Path("/user"));
        fileSystem.mkdirs(new Path("/tmp"));
        fileSystem.mkdirs(new Path("/hadoop/mapred/system"));
        fileSystem.setPermission(new Path("target/test-data"),
            FsPermission.valueOf("-rwxrwxrwx"));
        fileSystem.setPermission(new Path("target/test-data" + "/minicluster"),
            FsPermission.valueOf("-rwxrwxrwx"));
        fileSystem.setPermission(new Path("target/test-data"
            + "/minicluster/mapred"), FsPermission.valueOf("-rwxrwxrwx"));
        fileSystem.setPermission(new Path("/user"),
            FsPermission.valueOf("-rwxrwxrwx"));
        fileSystem.setPermission(new Path("/tmp"),
            FsPermission.valueOf("-rwxrwxrwx"));
        fileSystem.setPermission(new Path("/hadoop/mapred/system"),
            FsPermission.valueOf("-rwx------"));
        String nnURI = fileSystem.getUri().toString();
        int numDirs = 1;
        String[] racks = null;
        String[] hosts = null;
        mrCluster = new MiniMRCluster(0, 0, taskTrackers, nnURI, numDirs,
            racks, hosts, null, conf);
        JobConf jobConf = mrCluster.createJobConf();
        System.setProperty(SEARCH_TEST_JOB_TRACKER,
            jobConf.get("mapred.job.tracker"));
        System.setProperty(SEARCH_TEST_NAME_NODE,
            jobConf.get("fs.default.name"));
        ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
      } catch (Exception ex) {
        shutdownMiniCluster();
        throw ex;
      }
      new MiniClusterShutdownMonitor().start();
    }
  }
  protected FileSystem getFileSystem() throws IOException {
    return dfsCluster.getFileSystem();
  }

  protected static void shutdownMiniCluster() {
    try {
      if (mrCluster != null) {
        mrCluster.shutdown();
      }
    } catch (Exception ex) {
      System.out.println(ex);
    }
    try {
      if (dfsCluster != null) {
        dfsCluster.shutdown();
      }
    } catch (Exception ex) {
      System.out.println(ex);
    }
  }

  private static final AtomicLong LAST_TESTCASE_FINISHED = new AtomicLong();
  private static final AtomicInteger RUNNING_TESTCASES = new AtomicInteger();

  private static class MiniClusterShutdownMonitor extends Thread {

    public MiniClusterShutdownMonitor() {
      setDaemon(true);
    }

    public void run() {
      long shutdownWait = Long.parseLong(System.getProperty(
          TEST_MINICLUSTER_MONITOR_SHUTDOWN_WAIT, "10")) * 1000;
      LAST_TESTCASE_FINISHED.set(System.currentTimeMillis());
      while (true) {
        if (RUNNING_TESTCASES.get() == 0) {
          if (System.currentTimeMillis() - LAST_TESTCASE_FINISHED.get() > shutdownWait) {
            break;
          }
        }
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ex) {
          break;
        }
      }
      shutdownMiniCluster();
    }
  }

  /**
   * Returns a jobconf preconfigured to talk with the test cluster/minicluster.
   * 
   * @return a jobconf preconfigured to talk with the test cluster/minicluster.
   */
  protected JobConf createJobConf() {
    JobConf jobConf;
    if (mrCluster != null) {
      jobConf = mrCluster.createJobConf();
    } else {
      jobConf = new JobConf();
      jobConf.set("mapred.job.tracker", getJobTrackerUri());
      jobConf.set("fs.default.name", getNameNodeUri());
    }
    return jobConf;
  }

  /**
   * A 'closure' used by {@link XTestCase#executeWhileJobTrackerIsShutdown}
   * method.
   */
  public static interface ShutdownJobTrackerExecutable {

    /**
     * Execute some code
     * 
     * @throws Exception
     *           thrown if the executed code throws an exception.
     */
    public void execute() throws Exception;
  }

  /**
   * Execute some code, expressed via a {@link ShutdownJobTrackerExecutable},
   * while the JobTracker is shutdown. Once the code has finished, the
   * JobTracker is restarted (even if an exception occurs).
   * 
   * @param executable
   *          The ShutdownJobTrackerExecutable to execute while the JobTracker
   *          is shutdown
   */
  protected void executeWhileJobTrackerIsShutdown(
      ShutdownJobTrackerExecutable executable) {
    mrCluster.stopJobTracker();
    Exception ex = null;
    try {
      executable.execute();
    } catch (Exception e) {
      ex = e;
    } finally {
      mrCluster.startJobTracker();
    }
    if (ex != null) {
      throw new RuntimeException(ex);
    }
  }

}
