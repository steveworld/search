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
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.util.JarFinder;
import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.cloud.SolrZkClient;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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
@SuppressCodecs({"Lucene3x", "Lucene40"})
public class GoLiveMiniMRTest extends AbstractFullDistribZkTestBase {
  
  private static final String RESOURCES_DIR = "target/test-classes";
  private static final String DOCUMENTS_DIR = RESOURCES_DIR + "/test-documents";
  private static final File MINIMR_CONF_DIR = new File(RESOURCES_DIR + "/solr/minimr");
  
  private static final String SEARCH_ARCHIVES_JAR = JarFinder.getJar(MapReduceIndexerTool.class);
  
  private static MiniDFSCluster dfsCluster = null;
  private static MiniMRCluster mrCluster = null;
  private static int numRuns = 0;
 
  private final String inputAvroFile1;
  private final String inputAvroFile2;
  private final String inputAvroFile3;

  
  @Override
  public String getSolrHome() {
    return MINIMR_CONF_DIR.getAbsolutePath();
  }
  
  public GoLiveMiniMRTest() {
    this.inputAvroFile1 = "sample-statuses-20120521-100919.avro";
    this.inputAvroFile2 = "sample-statuses-20120906-141433.avro";
    this.inputAvroFile3 = "sample-statuses-20120906-141433-medium.avro";
    
    fixShardCount = true;
    sliceCount = 3;
    shardCount = 3;
  }
  
  private static boolean isYarn() {
    try {
      Job.class.getMethod("getCluster");
      return true;
    } catch (NoSuchMethodException e) {
      return false;
    }    
  }
  
  @BeforeClass
  public static void setupClass() throws Exception {
//    if (isYarn()) {
//      org.junit.Assume.assumeTrue(false); // ignore test on Yarn until CDH-10420 is fixed
//    }
    if (System.getProperty("hadoop.log.dir") == null) {
      System.setProperty("hadoop.log.dir", "target");
    }
    int taskTrackers = 2;
    int dataNodes = 2;
    
    JobConf conf = new JobConf();
    conf.set("dfs.block.access.token.enable", "false");
    conf.set("dfs.permissions", "true");
    conf.set("hadoop.security.authentication", "simple");
    
    
    createTempDir();
    System.setProperty("test.build.data", dataDir + File.separator + "hdfs" + File.separator + "build");
    System.setProperty("test.cache.data", dataDir + File.separator + "hdfs" + File.separator + "cache");
    
    dfsCluster = new MiniDFSCluster(conf, dataNodes, true, null);
    FileSystem fileSystem = dfsCluster.getFileSystem();
    fileSystem.mkdirs(new Path("/tmp"));
    fileSystem.mkdirs(new Path("/user"));
    fileSystem.mkdirs(new Path("/hadoop/mapred/system"));
    fileSystem.setPermission(new Path("/tmp"),
        FsPermission.valueOf("-rwxrwxrwx"));
    fileSystem.setPermission(new Path("/user"),
        FsPermission.valueOf("-rwxrwxrwx"));
    fileSystem.setPermission(new Path("/hadoop/mapred/system"),
        FsPermission.valueOf("-rwx------"));
    String nnURI = fileSystem.getUri().toString();
    int numDirs = 1;
    String[] racks = null;
    String[] hosts = null;
    
    mrCluster = new MiniMRCluster(0, 0, taskTrackers, nnURI, numDirs, racks,
        hosts, null, conf);
    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
  }
  
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    System.setProperty("host", "127.0.0.1");
    System.setProperty("numShards", Integer.toString(sliceCount));
    
    uploadConfFiles();
  }
  
  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    System.clearProperty("host");
    System.clearProperty("numShards");
    FileSystem.closeAll();
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
  
  private JobConf getJobConf() {
    return mrCluster.createJobConf();
  }
  
  @Test
  @Override
  public void testDistribSearch() throws Exception {
    super.testDistribSearch();
  }
  
  @Override
  public void doTest() throws Exception {
    
    waitForRecoveriesToFinish(false);
    
    FileSystem fs = dfsCluster.getFileSystem();
    Path inDir = fs.makeQualified(new Path(
        "/user/testing/testMapperReducer/input"));
    fs.delete(inDir, true);
    String DATADIR = "/user/testing/testMapperReducer/data";
    Path dataDir = fs.makeQualified(new Path(DATADIR));
    fs.delete(dataDir, true);
    Path outDir = fs.makeQualified(new Path(
        "/user/testing/testMapperReducer/output"));
    fs.delete(outDir, true);
    
    assertTrue(fs.mkdirs(inDir));
    Path INPATH = upAvroFile(fs, inDir, DATADIR, dataDir, inputAvroFile1);
    
    JobConf jobConf = getJobConf();
    // enable mapred.job.tracker = local to run in debugger and set breakpoints
    // jobConf.set("mapred.job.tracker", "local");
    jobConf.setMaxMapAttempts(1);
    jobConf.setMaxReduceAttempts(1);
    jobConf.setJar(SEARCH_ARCHIVES_JAR);
    
    MapReduceIndexerTool tool;
    int res;
    QueryResponse results;
    HttpSolrServer server = new HttpSolrServer(cloudJettys.get(0).url);

    String[] args = new String[] {
        "--files",
        RESOURCES_DIR + "/tika-config.xml",
        "--solrhomedir=" + MINIMR_CONF_DIR.getAbsolutePath(),
        "--outputdir=" + outDir.toString(),
        "--mappers=3",
        ++numRuns % 2 == 0 ? "--inputlist=" + INPATH.toString() : dataDir.toString(), 
        "--shardurl", cloudJettys.get(0).url, 
        "--shardurl", cloudJettys.get(1).url, 
        "--shardurl", cloudJettys.get(2).url, 
        "--golivethreads", Integer.toString(random().nextInt(15) + 1),
        "--verbose",
        "--golive"
    };
    
    if (true) {
      tool = new MapReduceIndexerTool();
      res = ToolRunner.run(jobConf, tool, args);
      assertEquals(0, res);
      assertTrue(tool.job.isComplete());
      assertTrue(tool.job.isSuccessful());
      results = server.query(new SolrQuery("*:*"));
      assertEquals(20, results.getResults().getNumFound());
    }    
    
    fs.delete(inDir, true);    
    INPATH = upAvroFile(fs, inDir, DATADIR, dataDir, inputAvroFile2);
    
    args = new String[] {
        "--files",
        RESOURCES_DIR + "/tika-config.xml",
        "--solrhomedir=" + MINIMR_CONF_DIR.getAbsolutePath(),
        "--outputdir=" + outDir.toString(),
        "--mappers=3",
        "--verbose",
        "--golive",
        ++numRuns % 2 == 0 ? "--inputlist=" + INPATH.toString() : dataDir.toString(), 
        "--shardurl", cloudJettys.get(0).url, 
        "--shardurl", cloudJettys.get(1).url, 
        "--shardurl", cloudJettys.get(2).url, 
        "--golivethreads", Integer.toString(random().nextInt(15) + 1)
    };
    
    if (true) {
      tool = new MapReduceIndexerTool();
      res = ToolRunner.run(jobConf, tool, args);
      assertEquals(0, res);
      assertTrue(tool.job.isComplete());
      assertTrue(tool.job.isSuccessful());      
      results = server.query(new SolrQuery("*:*"));
      assertEquals(32, results.getResults().getNumFound());
    }    
    
    cloudClient.deleteByQuery("*:*");
    cloudClient.commit();
    
    // try using zookeeper
    fs.delete(inDir, true);    
    INPATH = upAvroFile(fs, inDir, DATADIR, dataDir, inputAvroFile3);
    
    args = new String[] {
        "--files",
        RESOURCES_DIR + "/tika-config.xml",
        "--solrhomedir=" + MINIMR_CONF_DIR.getAbsolutePath(),
        "--outputdir=" + outDir.toString(),
        "--mappers=3",
        "--reducers=6",
        "--verbose",
        "--golive",
        ++numRuns % 2 == 0 ? "--inputlist=" + INPATH.toString() : dataDir.toString(), 
        "--zkhost", zkServer.getZkAddress(), 
        "--collection", "collection1"
    };

    if (true) {
      tool = new MapReduceIndexerTool();
      res = ToolRunner.run(jobConf, tool, args);
      assertEquals(0, res);
      assertTrue(tool.job.isComplete());
      assertTrue(tool.job.isSuccessful());
      
      results = server.query(new SolrQuery("*:*"));      
      assertEquals(2124, results.getResults().getNumFound());
    }    
    
    server.shutdown();
  }
  
  private Path upAvroFile(FileSystem fs, Path inDir, String DATADIR,
      Path dataDir, String localFile) throws IOException, UnsupportedEncodingException {
    Path INPATH = new Path(inDir, "input.txt");
    OutputStream os = fs.create(INPATH);
    Writer wr = new OutputStreamWriter(os, "UTF-8");
    wr.write(DATADIR + "/" + inputAvroFile1);
    wr.close();
    
    assertTrue(fs.mkdirs(dataDir));
    fs.copyFromLocalFile(new Path(DOCUMENTS_DIR, localFile), dataDir);
    return INPATH;
  }
  
  public JettySolrRunner createJetty(File solrHome, String dataDir,
      String shardList, String solrConfigOverride, String schemaOverride)
      throws Exception {
    
    JettySolrRunner jetty = new JettySolrRunner(solrHome.getAbsolutePath(),
        context, 0, solrConfigOverride, schemaOverride);

    jetty.setShards(shardList);
    URI uri = dfsCluster.getFileSystem().getUri();
    jetty.setDataDir(uri.toString().substring("hdfs:/".length(),
        uri.toString().length())
        + "/" + new File(dataDir).getName());
    
    if (System.getProperty("collection") == null) {
      System.setProperty("collection", "collection1");
    }
    
    jetty.start();
    
    System.clearProperty("solr.ulog.dir");
    System.clearProperty("collection");
    
    return jetty;
  }
  
  private static void putConfig(SolrZkClient zkClient, File solrhome, String name) throws Exception {
    putConfig(zkClient, solrhome, name, name);
  }
  
  private static void putConfig(SolrZkClient zkClient, File solrhome, String srcName, String destName)
      throws Exception {
    
    File file = new File(solrhome, "conf" + File.separator + srcName);
    if (!file.exists()) {
      // LOG.info("skipping " + file.getAbsolutePath() +
      // " because it doesn't exist");
      return;
    }
    
    String destPath = "/configs/conf1/" + destName;
    // LOG.info("put " + file.getAbsolutePath() + " to " + destPath);
    zkClient.makePath(destPath, file, false, true);
  }
  
  private void uploadConfFiles() throws Exception {
    // upload our own config files
    SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress(), 10000);
    putConfig(zkClient, new File(RESOURCES_DIR + "/solr/solrcloud"),
        "solrconfig.xml");
    putConfig(zkClient, MINIMR_CONF_DIR, "schema.xml");
    putConfig(zkClient, MINIMR_CONF_DIR, "elevate.xml");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_en.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_ar.txt");
    
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_bg.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_ca.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_cz.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_da.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_el.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_es.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_eu.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_de.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_fa.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_fi.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_fr.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_ga.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_gl.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_hi.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_hu.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_hy.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_id.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_it.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_ja.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_lv.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_nl.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_no.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_pt.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_ro.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_ru.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_sv.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_th.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stopwords_tr.txt");
    
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/contractions_ca.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/contractions_fr.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/contractions_ga.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/contractions_it.txt");
    
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/stemdict_nl.txt");
    
    putConfig(zkClient, MINIMR_CONF_DIR, "lang/hyphenations_ga.txt");
    
    putConfig(zkClient, MINIMR_CONF_DIR, "stopwords.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "protwords.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "currency.xml");
    putConfig(zkClient, MINIMR_CONF_DIR, "open-exchange-rates.json");
    putConfig(zkClient, MINIMR_CONF_DIR, "mapping-ISOLatin1Accent.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "old_synonyms.txt");
    putConfig(zkClient, MINIMR_CONF_DIR, "synonyms.txt");
    zkClient.close();
  }
}
