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

import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.hadoop.SolrDocumentConverter;
import org.apache.solr.hadoop.SolrInputDocumentWritable;
import org.apache.solr.hadoop.SolrOutputFormat;
import org.apache.solr.hadoop.SolrReducer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;

public class TikaMapperReducerTest extends Assert {
  private static final String RESOURCES_DIR = "target/test-classes";

  MapDriver<LongWritable, Text, Text, SolrInputDocumentWritable> mapDriver;
  ReduceDriver<Text, SolrInputDocumentWritable, Text, SolrInputDocumentWritable> reduceDriver;
  MapReduceDriver<LongWritable, Text, Text, SolrInputDocumentWritable, Text, SolrInputDocumentWritable> mapReduceDriver;

  static File solrHomeZip;

  MySolrReducer myReducer;

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
  public void setUp() {
    TikaMapper mapper = new TikaMapper();
    myReducer = new MySolrReducer();
    mapDriver = MapDriver.newMapDriver(mapper);;
    reduceDriver = ReduceDriver.newReduceDriver(myReducer);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, myReducer);

    Configuration config = mapDriver.getConfiguration();
    config.set(SolrOutputFormat.ZIP_NAME, solrHomeZip.getName());
    config = reduceDriver.getConfiguration();
    config.set(SolrOutputFormat.ZIP_NAME, solrHomeZip.getName());
    config = mapReduceDriver.getConfiguration();
    config.set(SolrOutputFormat.ZIP_NAME, solrHomeZip.getName());
  }

  public static class MySolrReducer extends SolrReducer {
    Context context;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      this.context = context;

      // handle a bug in MRUnit - should be fixed in MRUnit 1.0.0
      when(context.getTaskAttemptID()).thenAnswer(new Answer<TaskAttemptID>() {
        @Override
        public TaskAttemptID answer(final InvocationOnMock invocation) {
          return new TaskAttemptID(new TaskID(), 0);
        }
      });

      super.setup(context);
    }

  }

  @Test
  public void testMapper() throws Exception {
    mapDriver.withInput(new LongWritable(0L), new Text(new File("target/test-classes/sample-statuses-20120906-141433.avro").toURI().toString()));

    SolrInputDocument sid = new SolrInputDocument();
    sid.addField("id", "uniqueid1");
    sid.addField("user_name", "user1");
    sid.addField("text", "content of record one");
    SolrInputDocumentWritable sidw = new SolrInputDocumentWritable(sid);

    mapDriver
      .withCacheArchive(solrHomeZip.getAbsolutePath())
      .withOutput(new Text("0"), sidw);
    //mapDriver.runTest();
    List<Pair<Text, SolrInputDocumentWritable>> result = mapDriver.run();
    for (Pair<Text, SolrInputDocumentWritable> p: result) {
      System.out.println(p.getFirst());
      System.out.println(p.getSecond());
    }
  }

  public static class NullInputFormat<K, V> extends InputFormat<K, V> {
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException,
        InterruptedException {
      return Lists.newArrayList();
    }

    @Override
    public RecordReader<K, V> createRecordReader(InputSplit split,
        TaskAttemptContext context) throws IOException, InterruptedException {
      return null;
    }
    
  }

  @Test
  public void testReducer() throws Exception {
    SolrDocumentConverter.setSolrDocumentConverter(TikaDocumentConverter.class, reduceDriver.getContext().getConfiguration());

    List<SolrInputDocumentWritable> values = new ArrayList<SolrInputDocumentWritable>();
    SolrInputDocument sid = new SolrInputDocument();
    String id = "myid1";
    sid.addField("id", id);
    sid.addField("text", "some unique text");
    SolrInputDocumentWritable sidw = new SolrInputDocumentWritable(sid);
    values.add(sidw);
    reduceDriver.withInput(new Text(id), values);

    reduceDriver.withCacheArchive(solrHomeZip.getAbsolutePath());
    
    reduceDriver.withOutputFormat(SolrOutputFormat.class, NullInputFormat.class);

    reduceDriver.run();
  }

  @Test
  public void testMapReduce() throws IOException {
    mapReduceDriver.withInput(new LongWritable(0L), new Text(new File("target/test-classes/sample-statuses-20120906-141433.avro").toURI().toString()));

    mapReduceDriver.withCacheArchive(solrHomeZip.getAbsolutePath());

    SolrDocumentConverter.setSolrDocumentConverter(TikaDocumentConverter.class, mapReduceDriver.getConfiguration());
    mapReduceDriver.withOutputFormat(SolrOutputFormat.class, NullInputFormat.class);

    mapReduceDriver.run();
  }
  
  @Test
  public void testPath() {
    Path path = new Path("hdfs://c2202.halxg.cloudera.com:8020/user/foo/bar.txt");
    assertEquals("/user/foo/bar.txt", path.toUri().getPath());
    assertEquals("bar.txt", path.getName());
    assertEquals("hdfs", path.toUri().getScheme());
    assertEquals("c2202.halxg.cloudera.com:8020", path.toUri().getAuthority());
    
    path = new Path("/user/foo/bar.txt");
    assertEquals("/user/foo/bar.txt", path.toUri().getPath());
    assertEquals("bar.txt", path.getName());
    assertEquals(null, path.toUri().getScheme());
    assertEquals(null, path.toUri().getAuthority());
  }
  
}
