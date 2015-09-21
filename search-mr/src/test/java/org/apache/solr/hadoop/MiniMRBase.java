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
import java.lang.reflect.Array;
import java.net.URI;
import java.text.NumberFormat;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.util.JarFinder;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.cloud.AbstractZkTestCase;
import org.apache.solr.common.cloud.SolrZkClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MiniMRBase extends AbstractFullDistribZkTestBase {
  private static final Logger logger = LoggerFactory.getLogger(MiniMRBase.class);

  protected final String inputAvroFile1;
  protected final String inputAvroFile2;
  protected final String inputAvroFile3;

  protected static final String RESOURCES_DIR = getFile("morphlines-core.marker").getParent();  
  private static final File MINIMR_INSTANCE_DIR = new File(RESOURCES_DIR + "/solr/minimr");
  protected static final File MINIMR_CONF_DIR = new File(RESOURCES_DIR + "/solr/minimr");
  protected static final String DOCUMENTS_DIR = RESOURCES_DIR + "/test-documents";
  
  static String SEARCH_ARCHIVES_JAR;
  
  static MiniDFSCluster dfsCluster = null;
  static MiniMRCluster mrCluster = null;
  static String tempDir;
 
  static File solrHomeDirectory;

  public MiniMRBase() {
    super();
    
    this.inputAvroFile1 = "sample-statuses-20120521-100919.avro";
    this.inputAvroFile2 = "sample-statuses-20120906-141433.avro";
    this.inputAvroFile3 = "sample-statuses-20120906-141433-medium.avro";
    
    sliceCount = TEST_NIGHTLY ? 5 : 3;
    shardCount = sliceCount;
  }

  @BeforeClass
  public static void setupClass() throws Exception {
    System.setProperty("solr.hdfs.blockcache.global", Boolean.toString(LuceneTestCase.random().nextBoolean()));
    System.setProperty("solr.hdfs.blockcache.enabled", Boolean.toString(LuceneTestCase.random().nextBoolean()));
    System.setProperty("solr.hdfs.blockcache.blocksperbank", "2048");
    
    solrHomeDirectory = createTempDir();
  
    assumeFalse("HDFS tests were disabled by -Dtests.disableHdfs",
        Boolean.parseBoolean(System.getProperty("tests.disableHdfs", "false")));
    
    assumeFalse("FIXME: This test does not work with Windows because of native library requirements", Constants.WINDOWS);
    assumeTrue("This test has issues with locales with non-Arabic digits",
        "1".equals(NumberFormat.getInstance().format(1)));
    
    AbstractZkTestCase.SOLRHOME = solrHomeDirectory;
    FileUtils.copyDirectory(MINIMR_INSTANCE_DIR, AbstractZkTestCase.SOLRHOME);
    tempDir = createTempDir().getAbsolutePath();
  
    new File(tempDir).mkdirs();
  
    FileUtils.copyFile(new File(RESOURCES_DIR + "/custom-mimetypes.xml"), new File(tempDir + "/custom-mimetypes.xml"));
    
    UtilsForTests.setupMorphline(tempDir, "test-morphlines/solrCellDocumentTypes", true, RESOURCES_DIR);
    
    System.setProperty("hadoop.log.dir", new File(tempDir, "logs").getAbsolutePath());
    
    final int taskTrackers = 2;
    final int dataNodes = 2;
    
    JobConf conf = new JobConf();
    conf.set("dfs.block.access.token.enable", "false");
    conf.set("dfs.permissions", "true");
    conf.set("hadoop.security.authentication", "simple");
    conf.set("mapreduce.jobhistory.minicluster.fixed.ports", "false");
    conf.set("mapreduce.jobhistory.admin.address", "0.0.0.0:0");
    
    new File(tempDir + File.separator +  "nm-local-dirs").mkdirs();
    
    System.setProperty("test.build.dir", tempDir + File.separator + "hdfs" + File.separator + "test-build-dir");
    System.setProperty("test.build.data", tempDir + File.separator + "hdfs" + File.separator + "build");
    System.setProperty("test.cache.data", tempDir + File.separator + "hdfs" + File.separator + "cache");
  
    // Initialize AFTER test.build.dir is set, JarFinder uses it.
    SEARCH_ARCHIVES_JAR = JarFinder.getJar(MapReduceIndexerTool.class);
    
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
        
    mrCluster = new MiniMRCluster(0, 0, taskTrackers, nnURI, numDirs, racks, hosts, null, conf);

    ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
  }

  @Override
  public String getSolrHome() {
    return solrHomeDirectory.getPath();
  }

  @AfterClass
  public static void teardownClass() throws Exception {
    System.clearProperty("solr.hdfs.blockcache.global");
    System.clearProperty("solr.hdfs.blockcache.blocksperbank");
    System.clearProperty("solr.hdfs.blockcache.enabled");
    System.clearProperty("hadoop.log.dir");
    System.clearProperty("test.build.dir");
    System.clearProperty("test.build.data");
    System.clearProperty("test.cache.data");
    
    if (mrCluster != null) {
      mrCluster.shutdown();
      mrCluster = null;
    }
    if (dfsCluster != null) {
      dfsCluster.shutdown();
      dfsCluster = null;
    }
    FileSystem.closeAll();
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    System.setProperty("host", "127.0.0.1");
    System.setProperty("numShards", Integer.toString(sliceCount));
    URI uri = dfsCluster.getFileSystem().getUri();
    System.setProperty("solr.hdfs.home",  uri.toString() + "/" + this.getClass().getName());
    uploadConfFiles();
    System.setProperty("solr.tests.cloud.cm.enabled", "false"); // disable Solr ChaosMonkey
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    System.clearProperty("host");
    System.clearProperty("numShards");
    System.clearProperty("solr.hdfs.home");
  }

  protected static boolean putConfig(SolrZkClient zkClient, File solrhome, String name) throws Exception {
    return putConfig(zkClient, solrhome, name, name);
  }

  protected JobConf getJobConf() throws IOException {
    return mrCluster.createJobConf();
  }

  protected static boolean putConfig(SolrZkClient zkClient, File solrhome, String srcName, String destName) throws Exception {
    File file = new File(new File(solrhome, "conf"), srcName);
    if (!file.exists()) {
      logger.trace("skipping " + file.getAbsolutePath() + " because it doesn't exist");
      return false;
    }
    
    String destPath = "/configs/conf1/" + destName;
    logger.trace("put " + file.getAbsolutePath() + " to " + destPath);
    zkClient.makePath(destPath, file, false, true);
    return true;
  }
  
  
  void uploadConfFiles() throws Exception {
    // upload our own config files
    try (SolrZkClient zkClient = new SolrZkClient(zkServer.getZkAddress(), 10000)) {
      putConfig(zkClient, new File(RESOURCES_DIR + "/solr/solrcloud"), "solrconfig.xml");
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
    }
  }

  protected static <T> T[] concat(T[]... arrays) {
    if (arrays.length <= 0) {
      throw new IllegalArgumentException();
    }
    Class<? extends Object[]> clazz = null;
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