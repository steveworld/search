/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.util.JarFinder;
import org.apache.hadoop.util.ToolRunner;
import org.apache.solr.handler.extraction.ExtractingParams;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(value = Parameterized.class)
public class MorphlineBasicMiniMRTest extends Assert {
  
  private static final boolean ENABLE_LOCAL_JOB_RUNNER = false; // for debugging only
  private static final String RESOURCES_DIR = "target/test-classes";
  private static final String DOCUMENTS_DIR = RESOURCES_DIR + "/test-documents";
  private static final File MINIMR_CONF_DIR = new File(RESOURCES_DIR + "/solr/minimr");
  private static final String TIKA_CONFIG_FILE_NAME = MapReduceIndexerTool.TIKA_CONFIG_FILE_NAME;
  
  private static final String SEARCH_ARCHIVES_JAR = JarFinder.getJar(MapReduceIndexerTool.class);

  private static MiniDFSCluster dfsCluster = null;
  private static MiniMRCluster mrCluster = null;
  private static int numRuns = 0;

  private final String inputAvroFile;
  private final int count;
  
  protected MapReduceIndexerTool createTool() {
    MapReduceIndexerTool tool = new MapReduceIndexerTool();
    tool.enableMorphline();
    return tool;
  }

  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        { "sample-statuses-20120906-141433.avro", 2 },
        { "sample-statuses-20120521-100919.avro", 20 }, 
        { "sample-statuses-20120906-141433-medium.avro", 2104 }, 
    });
  }

  public MorphlineBasicMiniMRTest(String inputAvroFile, int count) {
    this.inputAvroFile = inputAvroFile;
    this.count = count;
  }

  @BeforeClass
  public static void setupClass() throws Exception {
    if (System.getProperty("hadoop.log.dir") == null) {
      System.setProperty("hadoop.log.dir", "target");
    }
    int taskTrackers = 2;
    int dataNodes = 2;
//    String proxyUser = System.getProperty("user.name");
//    String proxyGroup = "g";
//    StringBuilder sb = new StringBuilder();
//    sb.append("127.0.0.1,localhost");
//    for (InetAddress i : InetAddress.getAllByName(InetAddress.getLocalHost().getHostName())) {
//      sb.append(",").append(i.getCanonicalHostName());
//    }
    
    System.setProperty("solr.hdfs.blockcache.enabled", "false");

    JobConf conf = new JobConf();
    conf.set("dfs.block.access.token.enable", "false");
    conf.set("dfs.permissions", "true");
    conf.set("hadoop.security.authentication", "simple");

    dfsCluster = new MiniDFSCluster(conf, dataNodes, true, null);
    FileSystem fileSystem = dfsCluster.getFileSystem();
    fileSystem.mkdirs(new Path("/tmp"));
    fileSystem.mkdirs(new Path("/user"));
    fileSystem.mkdirs(new Path("/hadoop/mapred/system"));
    fileSystem.setPermission(new Path("/tmp"), FsPermission.valueOf("-rwxrwxrwx"));
    fileSystem.setPermission(new Path("/user"), FsPermission.valueOf("-rwxrwxrwx"));
    fileSystem.setPermission(new Path("/hadoop/mapred/system"), FsPermission.valueOf("-rwx------"));
    String nnURI = fileSystem.getUri().toString();
    int numDirs = 1;
    String[] racks = null;
    String[] hosts = null;

    mrCluster = new MiniMRCluster(0, 0, taskTrackers, nnURI, numDirs, racks, hosts, null, conf);
    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
  }

  @AfterClass
  public static void teardownClass() throws Exception {
    System.clearProperty("solr.hdfs.blockcache.enabled");
    if (mrCluster != null) {
      mrCluster.shutdown();
      mrCluster = null;
    }
    if (dfsCluster != null) {
      dfsCluster.shutdown();
      dfsCluster = null;
    }
  }
  
  @After
  public void tearDown() {
    if (ENABLE_LOCAL_JOB_RUNNER) {
      File tikaConfigFile = new File(TIKA_CONFIG_FILE_NAME);
      if (tikaConfigFile.exists()) {
        assertTrue(tikaConfigFile.delete());
      }
    }
  }

  private JobConf getJobConf() {
    return mrCluster.createJobConf();
  }

  @Test
  public void testPathParts() throws Exception { // see PathParts
    FileSystem fs = dfsCluster.getFileSystem();
    int dfsClusterPort = fs.getWorkingDirectory().toUri().getPort();
    assertTrue(dfsClusterPort > 0);
    JobConf jobConf = getJobConf();
    Configuration simpleConf = new Configuration();
    
    for (Configuration conf : Arrays.asList(jobConf, simpleConf)) {
      for (String queryAndFragment : Arrays.asList("", "?key=value#fragment")) {
        for (String up : Arrays.asList("", "../")) {
          String down = up.length() == 0 ? "foo/" : "";
          String uploadURL = "hdfs://localhost:12345/user/foo/" + up + "bar.txt" + queryAndFragment;
          PathParts parts = new PathParts(uploadURL, conf);
          assertEquals(uploadURL, parts.getUploadURL());
          assertEquals("/user/" + down + "bar.txt", parts.getURIPath());
          assertEquals("bar.txt", parts.getName());
          assertEquals("hdfs", parts.getScheme());
          assertEquals("localhost", parts.getHost());
          assertEquals(12345, parts.getPort());
          assertEquals("hdfs://localhost:12345/user/" + down + "bar.txt", parts.getId());
          assertEquals(parts.getId(), parts.getDownloadURL());
          assertFileNotFound(parts);
    
          uploadURL = "hdfs://localhost/user/foo/" + up + "bar.txt" + queryAndFragment;
          parts = new PathParts(uploadURL, conf);
          assertEquals(uploadURL, parts.getUploadURL());
          assertEquals("/user/" + down + "bar.txt", parts.getURIPath());
          assertEquals("bar.txt", parts.getName());
          assertEquals("hdfs", parts.getScheme());
          assertEquals("localhost", parts.getHost());
          assertEquals(8020, parts.getPort());
          assertEquals("hdfs://localhost:8020/user/" + down + "bar.txt", parts.getId());
          assertEquals(parts.getId(), parts.getDownloadURL());
          assertFileNotFound(parts);
        }
      }
    }    

    for (Configuration conf : Arrays.asList(jobConf)) {
      for (String queryAndFragment : Arrays.asList("", "?key=value#fragment")) {
        for (String up : Arrays.asList("", "../")) {
          // verify using absolute path
          String down = up.length() == 0 ? "foo/" : "";
          String uploadURL = "/user/foo/" + up + "bar.txt" + queryAndFragment;
          PathParts parts = new PathParts(uploadURL, conf);
          assertEquals(uploadURL, parts.getUploadURL());
          assertEquals("/user/" + down + "bar.txt", parts.getURIPath());
          assertEquals("bar.txt", parts.getName());
          assertEquals("hdfs", parts.getScheme());
          assertTrue("localhost".equals(parts.getHost()) || "localhost.localdomain".equals(parts.getHost()));
          assertEquals(dfsClusterPort, parts.getPort());
          assertTrue(parts.getId().equals("hdfs://localhost:" + dfsClusterPort + "/user/" + down + "bar.txt")
                  || parts.getId().equals("hdfs://localhost.localdomain:" + dfsClusterPort + "/user/" + down + "bar.txt")
          );
          assertFileNotFound(parts);          
          
          // verify relative path is interpreted to be relative to user's home dir and resolved to an absolute path
          uploadURL = "xuser/foo/" + up + "bar.txt" + queryAndFragment;
          parts = new PathParts(uploadURL, conf);
          assertEquals(uploadURL, parts.getUploadURL());
          String homeDir = "/user/" + System.getProperty("user.name");
          assertEquals(homeDir + "/xuser/" + down + "bar.txt", parts.getURIPath());
          assertEquals("bar.txt", parts.getName());
          assertEquals("hdfs", parts.getScheme());
          assertTrue("localhost".equals(parts.getHost()) || "localhost.localdomain".equals(parts.getHost()));
          assertEquals(dfsClusterPort, parts.getPort());
          assertTrue(parts.getId().equals("hdfs://localhost:" + dfsClusterPort + homeDir + "/xuser/" + down + "bar.txt")
                  || parts.getId().equals("hdfs://localhost.localdomain:" + dfsClusterPort + homeDir + "/xuser/" + down + "bar.txt")
          );
          assertFileNotFound(parts);
        }
      }
    }
    
    try {
      new PathParts("/user/foo/bar.txt", simpleConf);
      fail("host/port resolution requires minimr conf, not a simple conf");
    } catch (IllegalArgumentException e) {
      ; // expected
    }    
  }

  private void assertFileNotFound(PathParts parts) {
    try {
      parts.getFileSystem().getFileStatus(parts.getUploadPath());
      fail();
    } catch (IOException e) {
      ; // expected
    }
  }

  @Test
  public void mrRun() throws Exception {
    FileSystem fs = dfsCluster.getFileSystem();
    Path inDir = fs.makeQualified(new Path("/user/testing/testMapperReducer/input"));
    fs.delete(inDir, true);
    String DATADIR = "/user/testing/testMapperReducer/data";
    Path dataDir = fs.makeQualified(new Path(DATADIR));
    fs.delete(dataDir, true);
    Path outDir = fs.makeQualified(new Path("/user/testing/testMapperReducer/output"));
    fs.delete(outDir, true);

    assertTrue(fs.mkdirs(inDir));
    Path INPATH = new Path(inDir, "input.txt");
    OutputStream os = fs.create(INPATH);
    Writer wr = new OutputStreamWriter(os, "UTF-8");
    wr.write(DATADIR + "/" + inputAvroFile);
    wr.close();

    assertTrue(fs.mkdirs(dataDir));
    fs.copyFromLocalFile(new Path(DOCUMENTS_DIR, inputAvroFile), dataDir);
    
    JobConf jobConf = getJobConf();
    if (ENABLE_LOCAL_JOB_RUNNER) { // enable Hadoop LocalJobRunner; this enables to run in debugger and set breakpoints
      jobConf.set("mapred.job.tracker", "local");
      
      // this is necessary because LocalJobRunner does not seem to implement Hadoop distributed cache feature 
      FileUtils.copyFile(new File(RESOURCES_DIR + File.separator + TIKA_CONFIG_FILE_NAME), new File(TIKA_CONFIG_FILE_NAME)); 
    }
    jobConf.setMaxMapAttempts(1);
    jobConf.setMaxReduceAttempts(1);
    jobConf.setJar(SEARCH_ARCHIVES_JAR);
    jobConf.setBoolean(ExtractingParams.IGNORE_TIKA_EXCEPTION, false);
    
    int shards = 2;
    int maxReducers = Integer.MAX_VALUE;
    if (ENABLE_LOCAL_JOB_RUNNER) {
      // local job runner has a couple of limitations: only one reducer is supported and the DistributedCache doesn't work.
      // see http://blog.cloudera.com/blog/2009/07/advice-on-qa-testing-your-mapreduce-jobs/
      maxReducers = 1;
      shards = 1;
    }
    
    String[] args = new String[] {
//        "--files", RESOURCES_DIR + "/test-morphlines/solrCellDocumentTypes.conf", 
//        //    + "," + RESOURCES_DIR + "/org/apache/tika/mime/custom-mimetypes.xml",
//        "-D", "morphlineFile=solrCellDocumentTypes.conf",
        "--morphline-file=" + RESOURCES_DIR + "/test-morphlines/solrCellDocumentTypes.conf",
        "--morphline-id=morphline1",
        "--solr-home-dir=" + MINIMR_CONF_DIR.getAbsolutePath(),
        "--output-dir=" + outDir.toString(),
        "--shards=" + shards,
        "--verbose",
        numRuns % 2 == 0 ? "--input-list=" + INPATH.toString() : dataDir.toString(),
        numRuns % 3 == 0 ? "--reducers=" + shards : (numRuns % 3 == 1  ? "--reducers=-1" : "--reducers=" + Math.min(8, maxReducers))
    };
    if (numRuns % 3 == 2) {
      args = concat(args, new String[] {"--fanout=2"});
    }
    if (numRuns == 0) {
      // force (slow) MapReduce based randomization to get coverage for that as well
      args = concat(new String[] {"-D", MapReduceIndexerTool.MAIN_MEMORY_RANDOMIZATION_THRESHOLD + "=-1"}, args); 
    }
    MapReduceIndexerTool tool = createTool();
    int res = ToolRunner.run(jobConf, tool, args);
    assertEquals(0, res);
    Job job = tool.job;
    assertTrue(job.isComplete());
    assertTrue(job.isSuccessful());

    if (numRuns % 3 != 2) {
      // Only run this check if mtree merge is disabled.
      // With mtree merge enabled the BatchWriter counters aren't available anymore because 
      // variable "job" now refers to the merge job rather than the indexing job
      assertEquals("Invalid counter " + SolrRecordWriter.class.getName() + "." + SolrCounters.DOCUMENTS_WRITTEN,
          count, job.getCounters().findCounter(SolrCounters.class.getName(), SolrCounters.DOCUMENTS_WRITTEN.toString()).getValue());
    }
    
    // Check the output is as expected
    outDir = new Path(outDir, MapReduceIndexerTool.RESULTS_DIR);
    Path[] outputFiles = FileUtil.stat2Paths(fs.listStatus(outDir));

    System.out.println("outputfiles:" + Arrays.toString(outputFiles));

    TestUtils.validateSolrServerDocumentCount(MINIMR_CONF_DIR, fs, outDir, count, shards);
    numRuns++;
  }
  
  protected static <T> T[] concat(T[]... arrays) {
    if (arrays.length <= 0) {
      throw new IllegalArgumentException();
    }
    Class clazz = null;
    int length = 0;
    for (T[] array : arrays) {
      clazz = array.getClass();
      length += array.length;
    }
    T[] result = (T[]) Array.newInstance(clazz.getComponentType(), length);
    int pos = 0;
    for (T[] array : arrays) {
      System.arraycopy(array, 0, result, pos, array.length);
      pos += array.length;
    }
    return result;
  }

}
