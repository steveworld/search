/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.solr.tika;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.solr.tika.parser.StreamingSequenceFileParser;
import org.apache.solr.tika.parser.StreamingSequenceFileParser.KeyValueParserHandler;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.XHTMLContentHandler;
import org.apache.zookeeper.common.IOUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xml.sax.SAXException;

public class TestSequenceFileParser extends TikaIndexerTestBase {
  private Map<String,Integer> expectedRecords = new HashMap();

  @BeforeClass
  public static void beforeClass() throws Exception {
    myInitCore(DEFAULT_BASE_DIR);
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
    testDocumentTypesInternal(files, expectedRecords, indexer, false);
    HashMap<String, ExpectedResult> expectedResultMap =
      getExpectedOutputForSimpleCase(numRecords);
    
    testDocumentContent(expectedResultMap);
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
    for (Map.Entry<Text, Text> entry : getMetadataForSequenceFile().entrySet()) {
      HashSet<String> results = new HashSet<String>();
      results.add(entry.getValue().toString());
      ExpectedResult expectedResult =
        new ExpectedResult(results, ExpectedResult.CompareType.equals);
      map.put(entry.getKey().toString(), expectedResult);
    }
    for (int i = 0; i < numRecords; ++i) {
      map.put("key", getExpectedResult("key" + i));
      map.put("value", getExpectedResult("value" + i));
    }
    return map;
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
    HashMap<String, String> extraContext = new HashMap<String, String>();
    extraContext.put(StreamingSequenceFileParser.KEY_VALUE_PARSER_CLASS_PROP,
      TextMyWritableParserHandler.class.getName());
    SolrIndexer solrIndexer = super.getSolrIndexer(extraContext);
    testDocumentTypesInternal(files, expectedRecords, solrIndexer, false);
    HashMap<String, ExpectedResult> expectedResultMap =
      getExpectedOutputForCustomCase(numRecords);

    super.testDocumentContent(solrIndexer, expectedResultMap);
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
    for (Map.Entry<Text, Text> entry : getMetadataForSequenceFile().entrySet()) {
      HashSet<String> results = new HashSet<String>();
      results.add(entry.getValue().toString());
      ExpectedResult expectedResult =
        new ExpectedResult(results, ExpectedResult.CompareType.equals);
      map.put(entry.getKey().toString(), expectedResult);
    }
    for (int i = 0; i < numRecords; ++i) {
      map.put("key", getExpectedResult(TextMyWritableParserHandler.keyStr(new Text("key" + i))));
      map.put("value", getExpectedResult(TextMyWritableParserHandler.valueStr(new MyWritable("value",i))));
    }
    return map;
  }


  private void createMyWritableSequenceFile(File file, int numRecords) throws IOException {
    SequenceFile.Metadata metadata = new SequenceFile.Metadata(getMetadataForSequenceFile());
    FSDataOutputStream out = new FSDataOutputStream(new FileOutputStream(file), null);
    SequenceFile.Writer writer = null;
    try {
      writer = SequenceFile.createWriter(new Configuration(), out, Text.class, MyWritable.class,
        SequenceFile.CompressionType.NONE, null, metadata);
      for (int i = 0; i < numRecords; ++i) {
        writer.append(new Text("key" + i), new MyWritable("value", i));
      }
    } finally {
      IOUtils.closeStream(writer);
    }
  }

  private TreeMap<Text, Text> getMetadataForSequenceFile() {
    TreeMap<Text, Text> metadata = new TreeMap<Text, Text>();
    metadata.put(new Text("license"), new Text("Apache"));
    metadata.put(new Text("year"), new Text("2013"));
    return metadata;
  }

  private static class MyWritable implements WritableComparable {
    private String prefix;
    private int suffix;

    /**
     * Empty constructor for writable
     */
    public MyWritable() {
    }

    public MyWritable(String prefix, int suffix) {
      this.prefix = prefix;
      this.suffix = suffix;
    }

    public void readFields(DataInput in) throws IOException {
      this.prefix = in.readUTF();
      this.suffix = in.readInt();
    }
    public void write(DataOutput out) throws IOException {
      out.writeUTF(this.prefix);
      out.writeInt(this.suffix);
    }

    public int compareTo(Object o) {
      throw new NotImplementedException("not implemented!");
    }
 
    public String getPrefix() { return prefix; }
    public int getSuffix() { return suffix; }
  }

  private ExpectedResult getExpectedResult(String result) {
    HashSet<String> results = new HashSet<String>();
    results.add(result);
    return new ExpectedResult(results, ExpectedResult.CompareType.equals);
  }

  private static class TextMyWritableParserHandler implements KeyValueParserHandler {
    //public TextMyWritableParserHandler() {}
    /**
     * Parse the given key/value using the parse parameters
     */
    public void parseSeqFileKeyValue(SequenceFile.Reader reader, Object key, Object value,
        XHTMLContentHandler handler, Metadata metadata, ParseContext parseContext) throws IOException {
      assert reader.getKeyClass().equals(Text.class);
      assert reader.getValueClass().equals(MyWritable.class);
      try {
        handler.startElement("key");
        Text textKey = (Text)key;
        handler.characters(keyStr(textKey));
        handler.endElement("key");
        handler.startElement("value");
        MyWritable myWritableValue = (MyWritable)value;
        handler.characters(valueStr(myWritableValue));
        handler.endElement("value");
      } catch (SAXException saex) {
        throw new IOException(saex);
      }
    }

    public static String keyStr(Text key) {
      // do something recognizable
      return key.toString().toUpperCase();
    }
    public static String valueStr(MyWritable value) {
      // do something recognizable
      return value.getSuffix() + value.getPrefix();
    }
  }
}
