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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.solr.hadoop.BatchWriter;
import org.apache.solr.hadoop.SolrInputDocumentWritable;
import org.apache.solr.hadoop.SolrOutputFormat;
import org.apache.solr.hadoop.SolrRecordWriter;
import org.apache.solr.hadoop.tika.TikaReducerTest.MySolrReducer;
import org.apache.solr.hadoop.tika.TikaReducerTest.NullInputFormat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(value = Parameterized.class)
public class TikaMapperReducerTest extends MRUnitBase {
  private final String inputAvroFile;
  private final int count;

  @Parameters
  public static Collection<Object[]> data() {
    Object[][] data = new Object[][] {
        { "sample-statuses-20120521-100919.avro", 20 },
        { "sample-statuses-20120906-141433.avro", 2 } };
    return Arrays.asList(data);
  }

  public TikaMapperReducerTest(String inputAvroFile, int count) {
    this.inputAvroFile = inputAvroFile;
    this.count = count;
  }

  @Test
  public void testMapReduce() throws IOException {
    TikaMapper mapper = new TikaMapper();
    MySolrReducer myReducer = new MySolrReducer();
    MapReduceDriver<LongWritable, Text, Text, SolrInputDocumentWritable, Text, SolrInputDocumentWritable> mapReduceDriver;
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, myReducer);

    Configuration config = mapReduceDriver.getConfiguration();
    config.set(SolrOutputFormat.ZIP_NAME, solrHomeZip.getName());

    mapReduceDriver.withInput(new LongWritable(0L), new Text(new File("target/test-classes/", inputAvroFile).toURI().toString()));

    mapReduceDriver.withCacheArchive(solrHomeZip.getAbsolutePath());

    mapReduceDriver.withOutputFormat(SolrOutputFormat.class, NullInputFormat.class);

    mapReduceDriver.run();

    assertEquals("Invalid counter " + SolrRecordWriter.class.getName() + "." + BatchWriter.COUNTER_DOCUMENTS_WRITTEN,
        count, mapReduceDriver.getCounters().findCounter("SolrRecordWriter", BatchWriter.COUNTER_DOCUMENTS_WRITTEN).getValue());
  }
  
}
