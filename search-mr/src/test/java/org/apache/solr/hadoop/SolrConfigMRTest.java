package org.apache.solr.hadoop;

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

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.util.LuceneTestCase.Nightly;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.store.hdfs.HdfsDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakAction;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakAction.Action;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakZombies;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakZombies.Consequence;


@ThreadLeakAction({Action.WARN})
@ThreadLeakLingering(linger = 0)
@ThreadLeakZombies(Consequence.CONTINUE)
@ThreadLeakScope(Scope.NONE)
@SuppressSSL // SSL does not work with this test for currently unknown reasons
@Slow
@Nightly
public class SolrConfigMRTest extends MiniMRBase {
  private static final Logger logger = LoggerFactory.getLogger(SolrConfigMRTest.class);
  
  private static final int RECORD_COUNT = 20;
  
  private static final File DROPALL_CONF_DIR = new File(RESOURCES_DIR + "/solr/dropall");
  
  Path inDir, outDir;
  JobConf jobConf;
  String[] args;
  
  public SolrConfigMRTest() {
    super();
    
    sliceCount = 1;
    shardCount = 1;
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();

    FileSystem fs = dfsCluster.getFileSystem();
    inDir = fs.makeQualified(new Path("/user/testing/testSolrConfig/input"));
    fs.delete(inDir, true);
    outDir = fs.makeQualified(new Path("/user/testing/testSolrConfig/output"));
    fs.delete(outDir, true);
    
    assertTrue(fs.mkdirs(inDir));
    fs.copyFromLocalFile(new Path(DOCUMENTS_DIR, inputAvroFile1), inDir);
    
    jobConf = getJobConf();
    jobConf.set("jobclient.output.filter", "ALL");
    // enable mapred.job.tracker = local to run in debugger and set breakpoints
    // jobConf.set("mapred.job.tracker", "local");
    jobConf.setMaxMapAttempts(1);
    jobConf.setMaxReduceAttempts(1);
    jobConf.setSpeculativeExecution(false);
    jobConf.setJar(SEARCH_ARCHIVES_JAR);
        
    args = new String[] {
        "--morphline-file=" + tempDir + "/test-morphlines/solrCellDocumentTypes.conf",
        "--log4j=" + getFile("log4j.properties").getAbsolutePath(),
        "--output-dir=" + outDir.toString(),
        "--mappers=1",
        "--reducers=1",
        "--verbose",
        "--zk-host", zkServer.getZkAddress(),
        "--collection=" + DEFAULT_COLLECTION,
        inDir.toString()
    };
  }

  @Override
  public void tearDown() throws Exception {
    FileSystem fs = dfsCluster.getFileSystem();
    fs.delete(inDir, true);
    fs.delete(outDir, true);
    
    super.tearDown();
  }

  @Override
  public void doTest() throws Exception {
    FileSystem fs = dfsCluster.getFileSystem();
    waitForRecoveriesToFinish(false);

    fs.delete(outDir, true);
    useDefaultOverZk();

    fs.delete(outDir, true);
    useExplicitFlag();

    fs.delete(outDir, true);
    useSolrHomeDir();
  }

  public void useDefaultOverZk() throws Exception {
    putDropAllConf();
    runJob(args);
    verifyDocCount(RECORD_COUNT);
  }

  public void useExplicitFlag() throws Exception {
    putDropAllConf();
    String[] prepend = {"--use-zk-solrconfig.xml"};
    args = concat(prepend, args);
    
    runJob(args);
    verifyDocCount(0);
  }
  
  public void useSolrHomeDir() throws Exception {
    // zookeeper has good conf
    String[] prepend = {"--solr-home-dir=" + DROPALL_CONF_DIR.getAbsolutePath()};
    args = concat(prepend, args);
    
    runJob(args);
    verifyDocCount(0);
  }
  
  private void putDropAllConf() throws Exception {
    try (SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress(), 10000)) {
      assertTrue("failed to find solrconfig.xml to drop updates", putConfig(zkClient, DROPALL_CONF_DIR, "solrconfig.xml"));
    }
  }
  
  private void runJob(String[] args) throws Exception, IOException {
    MapReduceIndexerTool tool = new MapReduceIndexerTool();
    int res = ToolRunner.run(jobConf, tool, args);
    assertEquals(0, res);
    assertTrue(tool.job.isComplete());
    assertTrue(tool.job.isSuccessful());
  }

  private void verifyDocCount(int expected) throws SolrServerException, IOException {
    Path indexDir = makePath(outDir, "results", "part-00000", "data", "index");
    try (HdfsDirectory dir = new HdfsDirectory(indexDir, jobConf) {
      @Override
      public void close() throws IOException {
        // Don't close the underlying filesystem object because it is shared here
      }
    }) {
      String[] files = dir.listAll();
      String lastSegmentsFile = SegmentInfos.getLastCommitSegmentsFileName(files);
      assertNotNull("No segment file found in output", lastSegmentsFile);
      
      int docs = 0;
      SegmentInfos si = new SegmentInfos();
      si.read(dir, lastSegmentsFile);
      for (SegmentCommitInfo sci : si) {
        docs += sci.info.getDocCount();
      }
      assertEquals(expected, docs);
    }
  }
  
  private Path makePath(Path parent, String... children) {
    Path path = parent;
    for (String child : children) {
      path = new Path(path, child);
    }
    return path;
  }
}
