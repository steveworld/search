package org.apache.solr.hadoop.tika;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.util.Arrays;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.Utils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.util.JarFinder;
import org.apache.solr.hadoop.BatchWriter;
import org.apache.solr.hadoop.SolrInputDocumentWritable;
import org.apache.solr.hadoop.SolrOutputFormat;
import org.apache.solr.hadoop.SolrReducer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TikaMRMiniMRTest {
  private static final String RESOURCES_DIR = "target/test-classes";
  private static File solrHomeZip;

  public static final String SEARCH_ARCHIVES_JAR = JarFinder.getJar(TikaIndexerTool.class);

  private MiniDFSCluster dfsCluster = null;
  private MiniMRCluster mrCluster = null;

  @BeforeClass
  public static void setupClass() throws Exception {
    solrHomeZip = SolrOutputFormat.createSolrHomeZip(new File(RESOURCES_DIR + "/solr/collection1"));
    assertNotNull(solrHomeZip);
  }

  @AfterClass
  public static void teardownClass() throws Exception {
    solrHomeZip.delete();
  }

  @Before
  public void setUp() throws Exception {
    if (System.getProperty("hadoop.log.dir") == null) {
      System.setProperty("hadoop.log.dir", "target");
    }
    int taskTrackers = 2;
    int dataNodes = 2;
    String proxyUser = System.getProperty("user.name");
    String proxyGroup = "g";
    StringBuilder sb = new StringBuilder();
    sb.append("127.0.0.1,localhost");
    for (InetAddress i : InetAddress.getAllByName(InetAddress.getLocalHost().getHostName())) {
      sb.append(",").append(i.getCanonicalHostName());
    }

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

  protected JobConf getJobConf() {
    return mrCluster.createJobConf();
  }

  @After
  public void tearDown() throws Exception {
    if (mrCluster != null) {
      mrCluster.shutdown();
    }
    if (dfsCluster != null) {
      dfsCluster.shutdown();
    }
  }

  @Test
  public void mrRun() throws Exception {
    FileSystem fs = FileSystem.get(getJobConf());

    Path inDir = new Path("testing/testMapperReducer/input");
    String DATADIR = "testing/testMapperReducer/data";
    Path dataDir = new Path(DATADIR);
    Path outDir = new Path("testing/testMapperReducer/output");

    assertTrue(fs.mkdirs(inDir));
    Path INPATH = new Path(inDir, "input.txt");
    OutputStream os = fs.create(INPATH);
    Writer wr = new OutputStreamWriter(os);
    wr.write(DATADIR + "/sample-statuses-20120906-141433.avro");
    wr.close();

    assertTrue(fs.mkdirs(dataDir));
    fs.copyFromLocalFile(new Path("target/test-classes/sample-statuses-20120906-141433.avro"), dataDir);

    Job job = new Job(getJobConf());

    job.setMaxMapAttempts(1);
    job.setMaxReduceAttempts(1);
    //jobConf.setInt("mapred.map.tasks", 1);

    job.setJar(SEARCH_ARCHIVES_JAR);

    job.setInputFormatClass(NLineInputFormat.class);

    job.setMapperClass(TikaMapper.class);
    job.setReducerClass(SolrReducer.class);
    job.setNumReduceTasks(1);

    job.setOutputFormatClass(SolrOutputFormat.class);
    SolrOutputFormat.addSolrConfToDistributedCache(job, solrHomeZip);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(SolrInputDocumentWritable.class);


    FileInputFormat.setInputPaths(job, inDir);
    FileOutputFormat.setOutputPath(job, outDir);

    assertTrue(job.waitForCompletion(true));
    Assert.assertTrue(job.isComplete());
    Assert.assertTrue(job.isSuccessful());

    assertEquals("Expected 2 counter increment", 2, job.getCounters()
        .findCounter("SolrRecordWriter", BatchWriter.COUNTER_DOCUMENTS_WRITTEN).getValue());

    // Check the output is as expected
    Path[] outputFiles = FileUtil.stat2Paths(fs.listStatus(outDir));

    System.out.println("outputfiles:" + Arrays.toString(outputFiles));

  }

}
