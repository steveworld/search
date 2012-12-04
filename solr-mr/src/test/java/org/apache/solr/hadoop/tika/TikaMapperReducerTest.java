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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.solr.hadoop.BatchWriter;
import org.apache.solr.hadoop.SolrInputDocumentWritable;
import org.apache.solr.hadoop.SolrOutputFormat;
import org.apache.solr.hadoop.tika.TikaReducerTest.MySolrReducer;
import org.apache.solr.hadoop.tika.TikaReducerTest.NullInputFormat;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TikaMapperReducerTest extends Assert {
  private static final String RESOURCES_DIR = "target/test-classes";

  static File solrHomeZip;

  @BeforeClass
  public static void setupClass() throws Exception {
    solrHomeZip = SolrOutputFormat.createSolrHomeZip(new File(RESOURCES_DIR + "/solr/mrunit"));
    assertNotNull(solrHomeZip);
  }

  @AfterClass
  public static void teardownClass() throws Exception {
    solrHomeZip.delete();
  }

  @Test
  public void testMapReduce() throws IOException {
    TikaMapper mapper = new TikaMapper();
    MySolrReducer myReducer = new MySolrReducer();
    MapReduceDriver<LongWritable, Text, Text, SolrInputDocumentWritable, Text, SolrInputDocumentWritable> mapReduceDriver;
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, myReducer);

    Configuration config = mapReduceDriver.getConfiguration();
    config.set(SolrOutputFormat.ZIP_NAME, solrHomeZip.getName());

    mapReduceDriver.withInput(new LongWritable(0L), new Text(new File("target/test-classes/sample-statuses-20120906-141433.avro").toURI().toString()));

    mapReduceDriver.withCacheArchive(solrHomeZip.getAbsolutePath());

    mapReduceDriver.withOutputFormat(SolrOutputFormat.class, NullInputFormat.class);

    mapReduceDriver.run();

    assertEquals("Expected 2 counter increment", 2, mapReduceDriver.getCounters()
        .findCounter("SolrRecordWriter", BatchWriter.COUNTER_DOCUMENTS_WRITTEN).getValue());
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
