/* Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.hadoop.morphline.seqfile;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.solr.morphline.AbstractSolrMorphlineTest;
import org.apache.solr.morphline.SolrMorphlineTest;
import org.apache.zookeeper.common.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadSequenceFileTest extends AbstractSolrMorphlineTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(SolrMorphlineTest.class);

  private Map<String,Integer> expectedRecords = new HashMap();

  @Before
  public void setUp() throws Exception {
    super.setUp();

    String path = RESOURCES_DIR + "/test-documents";
    expectedRecords.put(path + "/testSequenceFileContentSimple.seq", 5);
    expectedRecords.put(path + "/testSequenceFileContentCustomParsers.seq", 10);
  }

  /**
   * Test that Solr queries on a parsed SequenceFile document
   * return the expected content and fields.  Don't pass
   * in our own parser via the context.
   */
  @Test
  public void testSequenceFileContentSimple() throws Exception {
    String path = RESOURCES_DIR + "/test-documents";
    File sequenceFile = new File(path, "testSequenceFileContentSimple.seq");
    int numRecords = 5;
    createTextSequenceFile(sequenceFile, numRecords);
    String[] files = new String[] {
      sequenceFile.getAbsolutePath(),
    };
    expectedRecords.put(sequenceFile.getAbsolutePath(), numRecords);

    morphline = createMorphline("test-morphlines/sequenceFileMorphlineSimple");
    testDocumentTypesInternal(files, expectedRecords);
    HashMap<String, ExpectedResult> expectedResultMap =
      getExpectedOutputForSimpleCase(numRecords);
    
    testDocumentContent(expectedResultMap);
  }

  private void createTextSequenceFile(File file, int numRecords) throws IOException {
    SequenceFile.Metadata metadata = new SequenceFile.Metadata(getMetadataForSequenceFile());
    FSDataOutputStream out = new FSDataOutputStream(new FileOutputStream(file), null);
    SequenceFile.Writer writer = null;
    try {
      writer = SequenceFile.createWriter(new Configuration(), out, Text.class, Text.class,
        SequenceFile.CompressionType.NONE, null, metadata);
      for (int i = 0; i < numRecords; ++i) {
        writer.append(new Text("key" + i), new Text("value" + i));
      }
    } finally {
      IOUtils.closeStream(writer);
    }
  }

  /**
   * Returns a HashMap of expectedKey -> expectedValue.
   * File format is required to alternate lines of keys and values.
   *
   * @param file
   * @throws IOException
   */
  private HashMap<String, ExpectedResult> getExpectedOutputForSimpleCase(int numRecords)
  throws IOException{
    HashMap<String, ExpectedResult> map = new HashMap<String, ExpectedResult>();
    // ensure we get at least the last record
    map.put("key", getExpectedResult("key" + (numRecords - 1)));
    map.put("value", getExpectedResult("value" + (numRecords - 1)));
    return map;
  }

  /**
   * Test that Solr queries on a parsed SequenceFile document
   * return the expected content and fields.  Pass in our own
   * parser class for the key/value types via the context.
   */
  @Test
  public void testSequenceFileContentCustomParsers() throws Exception {
    String path = RESOURCES_DIR + "/test-documents";
    File sequenceFile = new File(path, "testSequenceFileContentCustomParsers.seq");
    int numRecords = 10;
    createMyWritableSequenceFile(sequenceFile, numRecords);
    String[] files = new String[] {
      sequenceFile.getAbsolutePath(),
    };
    expectedRecords.put(sequenceFile.getAbsolutePath(), numRecords);

    morphline = createMorphline("test-morphlines/sequenceFileMorphlineCustom");
    testDocumentTypesInternal(files, expectedRecords);
    HashMap<String, ExpectedResult> expectedResultMap =
      getExpectedOutputForCustomCase(numRecords);

    super.testDocumentContent(expectedResultMap);
  }

  private void createMyWritableSequenceFile(File file, int numRecords) throws IOException {
    SequenceFile.Metadata metadata = new SequenceFile.Metadata(getMetadataForSequenceFile());
    FSDataOutputStream out = new FSDataOutputStream(new FileOutputStream(file), null);
    SequenceFile.Writer writer = null;
    try {
      writer = SequenceFile.createWriter(new Configuration(), out, Text.class, ParseTextMyWritableBuilder.MyWritable.class,
        SequenceFile.CompressionType.NONE, null, metadata);
      for (int i = 0; i < numRecords; ++i) {
        writer.append(new Text("key" + i), new ParseTextMyWritableBuilder.MyWritable("value", i));
      }
    } finally {
      IOUtils.closeStream(writer);
    }
  }

  /**
   * Returns a HashMap of expectedKey -> expectedValue.
   * File format is required to alternate lines of keys and values.
   *
   * @param file
   * @throws IOException
   */
  private HashMap<String, ExpectedResult> getExpectedOutputForCustomCase(int numRecords)
  throws IOException{
    HashMap<String, ExpectedResult> map = new HashMap<String, ExpectedResult>();
    // ensure we get at least the last record
    map.put("key", getExpectedResult(
      ParseTextMyWritableBuilder.MyWritable.keyStr(new Text("key" + (numRecords - 1)))));
    map.put("value", getExpectedResult(
      ParseTextMyWritableBuilder.MyWritable.valueStr(new ParseTextMyWritableBuilder.MyWritable("value", numRecords - 1))));
    return map;
  }

  private ExpectedResult getExpectedResult(String result) {
    HashSet<String> results = new HashSet<String>();
    results.add(result);
    return new ExpectedResult(results, ExpectedResult.CompareType.equals);
  }

  private TreeMap<Text, Text> getMetadataForSequenceFile() {
    TreeMap<Text, Text> metadata = new TreeMap<Text, Text>();
    metadata.put(new Text("license"), new Text("Apache"));
    metadata.put(new Text("year"), new Text("2013"));
    return metadata;
  }
}
