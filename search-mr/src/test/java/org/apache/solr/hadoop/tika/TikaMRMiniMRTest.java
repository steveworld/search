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
package org.apache.solr.hadoop.tika;

import java.io.File;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
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
import org.apache.solr.hadoop.BatchWriter;
import org.apache.solr.hadoop.SolrRecordWriter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(value = Parameterized.class)
public class TikaMRMiniMRTest extends Assert {
  
  private static final boolean ENABLE_LOCAL_JOB_RUNNER = false; // for debugging only
  private static final String RESOURCES_DIR = "target/test-classes";
  private static final String DOCUMENTS_DIR = RESOURCES_DIR + "/test-documents";
  private static final File MINIMR_CONF_DIR = new File(RESOURCES_DIR + "/solr/minimr");
  private static final String TIKA_CONFIG_FILE_NAME = "tika-config.xml";
  
  private static final String SEARCH_ARCHIVES_JAR = JarFinder.getJar(TikaIndexerTool.class);

  private static MiniDFSCluster dfsCluster = null;
  private static MiniMRCluster mrCluster = null;
  private static int numRuns = 0;

  private final String inputAvroFile;
  private final int count;

  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        { "sample-statuses-20120906-141433.avro", 2 },
        { "sample-statuses-20120521-100919.avro", 20 }, 
        { "sample-statuses-20120906-141433-medium.avro", 2104 }, 
    });
  }

  public TikaMRMiniMRTest(String inputAvroFile, int count) {
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
    
    int shards = 2;
    String[] args = new String[] {
        "--files", RESOURCES_DIR + File.separator + TIKA_CONFIG_FILE_NAME,
        "--solrhomedir=" + MINIMR_CONF_DIR.getAbsolutePath(),
        "--outputdir=" + outDir.toString(),
        "--shards=" + shards,
        "--verbose",
        numRuns % 2 == 0 ? "--inputlist=" + INPATH.toString() : dataDir.toString(),
        numRuns % 3 == 0 ? "--reducers=" + shards : (numRuns % 3 == 1  ? "--reducers=-1" : "--reducers=8")
    };
    if (numRuns % 3 == 2) {
      args = concat(args, new String[] {"--fanout=2"});
    }
    TikaIndexerTool tool = new TikaIndexerTool();
    int res = ToolRunner.run(jobConf, tool, args);
    assertEquals(0, res);
    Job job = tool.job;
    assertTrue(job.isComplete());
    assertTrue(job.isSuccessful());

    if (numRuns % 3 != 2) {
      // Only run this check if mtree merge is disabled.
      // With mtree merge enabled the BatchWriter counters aren't available anymore because 
      // variable "job" now refers to the merge job rather than the indexing job
      assertEquals("Invalid counter " + SolrRecordWriter.class.getName() + "." + BatchWriter.COUNTER_DOCUMENTS_WRITTEN,
          count, job.getCounters().findCounter("SolrRecordWriter", BatchWriter.COUNTER_DOCUMENTS_WRITTEN).getValue());
    }
    
    // Check the output is as expected
    outDir = new Path(outDir, TikaIndexerTool.RESULTS_DIR);
    Path[] outputFiles = FileUtil.stat2Paths(fs.listStatus(outDir));

    System.out.println("outputfiles:" + Arrays.toString(outputFiles));

    Utils.validateSolrServerDocumentCount(MINIMR_CONF_DIR, fs, outDir, count, shards);
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
