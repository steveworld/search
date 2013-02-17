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
package org.apache.solr.hadoop;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.solr.hadoop.MapReduceIndexerTool;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapReduceIndexerToolArgumentParserTest extends Assert {
  private static final Logger LOG = LoggerFactory.getLogger(MapReduceIndexerToolArgumentParserTest.class);
  
  private FileSystem fs; 
  private MapReduceIndexerTool.MyArgumentParser parser;
  private MapReduceIndexerTool.Options opts;
  private PrintStream oldSystemOut;
  private PrintStream oldSystemErr;
  private ByteArrayOutputStream bout;
  private ByteArrayOutputStream berr;
  
  @Before
  public void setUp() throws IOException {
    fs = FileSystem.get(new Configuration());
    parser = new MapReduceIndexerTool.MyArgumentParser();
    opts = new MapReduceIndexerTool.Options();
    oldSystemOut = System.out;
    bout = new ByteArrayOutputStream();
    System.setOut(new PrintStream(bout, true, "UTF-8"));
    oldSystemErr = System.err;
    berr = new ByteArrayOutputStream();
    System.setErr(new PrintStream(berr, true, "UTF-8"));
  }

  @After
  public void tearDown() {
    System.setOut(oldSystemOut);
    System.setErr(oldSystemErr);
  }

  @Test
  public void testArgsParserTypicalUse() {
    String[] args = new String[] { 
        "--inputlist", "file:///tmp",
        "--outputdir", "file:/tmp/foo",
        "--solrhomedir", "/", 
        "--mappers", "10", 
        "--reducers", "9", 
        "--fanout", "8", 
        "--maxsegments", "7", 
        "--shards", "1",
        "--verbose", 
        "file:///home",
        "file:///dev",
        };
    Integer res = parser.parseArgs(args, fs, opts);
    assertNull(res != null ? res.toString() : "", res);
    assertEquals(Collections.singletonList(new Path("file:///tmp")), opts.inputLists);
    assertEquals(new Path("file:/tmp/foo"), opts.outputDir);
    assertEquals(new File("/"), opts.solrHomeDir);
    assertEquals(10, opts.mappers);
    assertEquals(9, opts.reducers);
    assertEquals(8, opts.fanout);
    assertEquals(7, opts.maxSegments);
    assertEquals(new Integer(1), opts.shards);
    assertEquals(null, opts.fairSchedulerPool);
    assertTrue(opts.isVerbose);
    assertTrue(opts.isRandomize);
    assertEquals(Arrays.asList(new Path("file:///home"), new Path("file:///dev")), opts.inputFiles);
    assertEmptySystemErrAndEmptySystemOut();
  }

  @Test
  public void testArgsParserMultipleSpecsOfSameKind() {
    String[] args = new String[] { 
        "--inputlist", "file:///tmp",
        "--inputlist", "file:///",
        "--outputdir", "file:/tmp/foo",
        "--solrhomedir", "/", 
        "--shards", "1",
        "file:///home",
        "file:///dev",
        };
    assertNull(parser.parseArgs(args, fs, opts));
    assertEquals(Arrays.asList(new Path("file:///tmp"), new Path("file:///")), opts.inputLists);
    assertEquals(Arrays.asList(new Path("file:///home"), new Path("file:///dev")), opts.inputFiles);
    assertEquals(new Path("file:/tmp/foo"), opts.outputDir);
    assertEquals(new File("/"), opts.solrHomeDir);
    assertEmptySystemErrAndEmptySystemOut();
  }

  @Test
  public void testArgsParserTypicalUseWithEqualsSign() {
    String[] args = new String[] { 
        "--inputlist=file:///tmp",
        "--outputdir=file:/tmp/foo",
        "--solrhomedir=/", 
        "--mappers=10", 
        "--shards", "1",
        "--verbose", 
        "file:///home",
        "file:///dev",
        };
    assertNull(parser.parseArgs(args, fs, opts));
    assertEquals(Collections.singletonList(new Path("file:///tmp")), opts.inputLists);
    assertEquals(new Path("file:/tmp/foo"), opts.outputDir);
    assertEquals(new File("/"), opts.solrHomeDir);
    assertEquals(10, opts.mappers);
    assertEquals(new Integer(1), opts.shards);
    assertEquals(null, opts.fairSchedulerPool);
    assertTrue(opts.isVerbose);
    assertTrue(opts.isRandomize);
    assertEquals(Arrays.asList(new Path("file:///home"), new Path("file:///dev")), opts.inputFiles);
    assertEmptySystemErrAndEmptySystemOut();
  }

  @Test
  public void testArgsParserMultipleSpecsOfSameKindWithEqualsSign() {
    String[] args = new String[] { 
        "--inputlist=file:///tmp",
        "--inputlist=file:///",
        "--outputdir=file:/tmp/foo",
        "--solrhomedir=/", 
        "--shards", "1",
        "file:///home",
        "file:///dev",
        };
    assertNull(parser.parseArgs(args, fs, opts));
    assertEquals(Arrays.asList(new Path("file:///tmp"), new Path("file:///")), opts.inputLists);
    assertEquals(Arrays.asList(new Path("file:///home"), new Path("file:///dev")), opts.inputFiles);
    assertEquals(new Path("file:/tmp/foo"), opts.outputDir);
    assertEquals(new File("/"), opts.solrHomeDir);
    assertEmptySystemErrAndEmptySystemOut();
  }

  @Test
  public void testArgsParserHelp() throws UnsupportedEncodingException  {
    String[] args = new String[] { "--help" };
    assertEquals(new Integer(0), parser.parseArgs(args, fs, opts));
    String helpText = new String(bout.toByteArray(), "UTF-8");
    assertTrue(helpText.contains("MapReduce batch job driver that creates "));
    assertTrue(helpText.contains("bin/hadoop command"));
    assertEquals(0, berr.toByteArray().length);
  }
  
  @Test
  public void testArgsParserOk() {
    String[] args = new String[] { 
        "--inputlist", "file:///tmp",
        "--outputdir", "file:/tmp/foo",
        "--solrhomedir", "/", 
        "--shards", "1",
        };
    assertNull(parser.parseArgs(args, fs, opts));
    assertEquals(new Integer(1), opts.shards);
    assertEmptySystemErrAndEmptySystemOut();
  }

  @Test
  public void testArgsParserUnknownArgName() {
    String[] args = new String[] { 
        "--xxxxxxxxinputlist", "file:///tmp",
        "--outputdir", "file:/tmp/foo",
        "--solrhomedir", "/", 
        };
    assertArgumentParserException(args);
  }

  @Test
  public void testArgsParserFileNotFound1() {
    String[] args = new String[] { 
        "--inputlist", "file:///tmp",
        "--outputdir", "file:/fileNotFound/foo",
        "--solrhomedir", "/", 
        };
    assertArgumentParserException(args);
  }
  
  @Test
  public void testArgsParserFileNotFound2() {
    String[] args = new String[] { 
        "--inputlist", "file:///tmp",
        "--outputdir", "file:/tmp/foo",
        "--solrhomedir", "/fileNotFound", 
        };
    assertArgumentParserException(args);
  }
  
  @Test
  public void testArgsParserIntOutOfRange() {
    String[] args = new String[] { 
        "--inputlist", "file:///tmp",
        "--outputdir", "file:/tmp/foo",
        "--solrhomedir", "/", 
        "--mappers", "-20"
        };
    assertArgumentParserException(args);
  }
  
  @Test
  public void testArgsParserIllegalFanout() {
    String[] args = new String[] { 
        "--inputlist", "file:///tmp",
        "--outputdir", "file:/tmp/foo",
        "--solrhomedir", "/", 
        "--fanout", "1" // must be >= 2
        };
    assertArgumentParserException(args);
  }
  
  @Test
  public void testArgsShardUrlOk() {
    String[] args = new String[] { 
        "--inputlist", "file:///tmp",
        "--outputdir", "file:/tmp/foo",
        "--solrhomedir", "/", 
        "--shardurl", "http://localhost:8983/solr/collection1",
        "--shardurl", "http://localhost:8983/solr/collection2",
        };
    assertNull(parser.parseArgs(args, fs, opts));
    assertEquals(Arrays.asList("http://localhost:8983/solr/collection1", "http://localhost:8983/solr/collection2"), opts.shardUrls);
    assertEquals(new Integer(2), opts.shards);
    assertEmptySystemErrAndEmptySystemOut();
  }
  
  @Test
  public void testArgsShardUrlMustHaveAParam() {
    String[] args = new String[] { 
        "--inputlist", "file:///tmp",
        "--outputdir", "file:/tmp/foo",
        "--solrhomedir", "/", 
        "--shardurl",
        };
    assertArgumentParserException(args);
  }
  
  @Test
  public void testArgsShardUrlAndShardsAreMutuallyExclusive() {
    String[] args = new String[] { 
        "--inputlist", "file:///tmp",
        "--outputdir", "file:/tmp/foo",
        "--solrhomedir", "/", 
        "--shards", "1", 
        "--shardurl", "http://localhost:8983/solr/collection1",
        };
    assertArgumentParserException(args);
  }
  
  @Test
  public void testArgsShardUrlNoGoLive() {
    String[] args = new String[] { 
        "--inputlist", "file:///tmp",
        "--outputdir", "file:/tmp/foo",
        "--solrhomedir", "/", 
        "--shardurl", "http://localhost:8983/solr/collection1"
        };
    assertNull(parser.parseArgs(args, fs, opts));
    assertEmptySystemErrAndEmptySystemOut();
    assertEquals(new Integer(1), opts.shards);
  }
  
  @Test
  public void testArgsShardUrlsAndZkhostAreMutuallyExclusive() {
    String[] args = new String[] { 
        "--inputlist", "file:///tmp",
        "--outputdir", "file:/tmp/foo",
        "--solrhomedir", "/", 
        "--shardurl", "http://localhost:8983/solr/collection1",
        "--shardurl", "http://localhost:8983/solr/collection1",
        "--zkhost", "http://localhost:2185",
        "--golive"
        };
    assertArgumentParserException(args);
  }
  
  @Test
  public void testArgsGoLiveAndSolrUrl() {
    String[] args = new String[] { 
        "--inputlist", "file:///tmp",
        "--outputdir", "file:/tmp/foo",
        "--solrhomedir", "/", 
        "--shardurl", "http://localhost:8983/solr/collection1",
        "--shardurl", "http://localhost:8983/solr/collection1",
        "--golive"
        };
    Integer result = parser.parseArgs(args, fs, opts);
    assertNull(result);
    assertEmptySystemErrAndEmptySystemOut();
  }
  
  @Test
  public void testArgsZkHostNoGoLive() {
    String[] args = new String[] { 
        "--inputlist", "file:///tmp",
        "--outputdir", "file:/tmp/foo",
        "--solrhomedir", "/", 
        "--zkhost", "http://localhost:2185",
        };
    assertArgumentParserException(args);
  }
  
  @Test
  public void testArgsGoLiveZkHostNoCollection() {
    String[] args = new String[] { 
        "--inputlist", "file:///tmp",
        "--outputdir", "file:/tmp/foo",
        "--solrhomedir", "/", 
        "--zkhost", "http://localhost:2185",

        "--golive"
        };
    assertArgumentParserException(args);
  }
  
  @Test
  public void testArgsGoLiveNoZkHostOrSolrUrl() {
    String[] args = new String[] { 
        "--inputlist", "file:///tmp",
        "--outputdir", "file:/tmp/foo",
        "--solrhomedir", "/", 
        "--golive"
        };
    assertArgumentParserException(args);
  }  
  
  private void assertEmptySystemErrAndEmptySystemOut() {
    assertEquals(0, bout.toByteArray().length);
    assertEquals(0, berr.toByteArray().length);
  }
  
  private void assertArgumentParserException(String[] args) {
    assertEquals("should have returned fail code", new Integer(1), parser.parseArgs(args, fs, opts));
    assertEquals("no sys out expected", 0, bout.toByteArray().length);
    String usageText;
    try {
      usageText = new String(berr.toByteArray(), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("unreachable");
    }
    
    assertTrue("should start with usage msg:" + usageText, usageText.startsWith("usage: hadoop "));
  }
  
}
