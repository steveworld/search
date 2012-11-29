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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Arrays;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Utils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.JarFinder;
import org.apache.solr.hadoop.SolrInputDocumentWritable;
import org.apache.solr.hadoop.SolrOutputFormat;
import org.apache.solr.hadoop.SolrReducer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;


public class MiniDfsMrMapReduceTest extends MiniDfsMRBase {
  private static final String RESOURCES_DIR = "target/test-classes";
  private static File solrHomeZip;

  public static final String SEARCH_ARCHIVES_JAR = JarFinder.getJar(TikaIndexer.class);

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
  @Override
  public void setup() throws Exception {
    super.setup();
  }
  @After
  public void teardown() throws Exception {
    super.shutdownMiniCluster();
  }

  @Ignore
  @Test
  public void testMapperReducer() throws Exception {
    Job job = new Job(createJobConf());

    job.setJar(SEARCH_ARCHIVES_JAR);

    FileSystem fs = getFileSystem();

    Path inDir = new Path("testing/testMapperReducer/input");
    String DATADIR = "testing/testMapperReducer/data";
    Path dataDir = new Path(DATADIR);
    Path outDir = new Path("testing/testMapperReducer/output");

    Path INPATH = new Path(inDir, "input.txt");
    OutputStream os = fs.create(INPATH);
    Writer wr = new OutputStreamWriter(os);
    wr.write(DATADIR + "/sample-statuses-20120906-141433.avro");
    wr.close();

    fs.copyFromLocalFile(new Path("target/test-classes/sample-statuses-20120906-141433.avro"), dataDir);

    job.setJobName("mr");

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

    // Check the output is as expected
    Path[] outputFiles = FileUtil.stat2Paths(
            fs.listStatus(outDir, new Utils.OutputFileUtils.OutputFilesFilter()));

    System.out.println(Arrays.toString(outputFiles));
  }
}
