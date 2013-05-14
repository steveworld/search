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
import org.apache.hadoop.fs.Path;
import org.apache.solr.hadoop.dedup.NoChangeUpdateConflictResolver;
import org.apache.solr.hadoop.dedup.RetainMostRecentUpdateConflictResolver;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapReduceIndexerToolArgumentParserTest extends Assert {
  
  private Configuration conf; 
  private MapReduceIndexerTool.MyArgumentParser parser;
  private MapReduceIndexerTool.Options opts;
  private PrintStream oldSystemOut;
  private PrintStream oldSystemErr;
  private ByteArrayOutputStream bout;
  private ByteArrayOutputStream berr;
  
  private static final String RESOURCES_DIR = "target/test-classes";  
  private static final String SOLR_HOME_DIR = "target/test-classes/solr/minimr";
  private static final String MORPHLINE_FILE = RESOURCES_DIR + "/test-morphlines/solrCellDocumentTypes.conf";
    
  private static final Logger LOG = LoggerFactory.getLogger(MapReduceIndexerToolArgumentParserTest.class);

  @Before
  public void setUp() throws IOException {
    conf = new Configuration();
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
        "--input-list", "file:///tmp",
        "--morphline-file", MORPHLINE_FILE,
        "--morphline-id", "morphline_xyz",
        "--output-dir", "file:/tmp/foo",
        "--solr-home-dir", SOLR_HOME_DIR,
        "--mappers", "10", 
        "--reducers", "9", 
        "--fanout", "8", 
        "--max-segments", "7", 
        "--shards", "1",
        "--verbose", 
        "file:///home",
        "file:///dev",
        };
    Integer res = parser.parseArgs(args, conf, opts);
    assertNull(res != null ? res.toString() : "", res);
    assertEquals(Collections.singletonList(new Path("file:///tmp")), opts.inputLists);
    assertEquals(new Path("file:/tmp/foo"), opts.outputDir);
    assertEquals(new File(SOLR_HOME_DIR), opts.solrHomeDir);
    assertEquals(10, opts.mappers);
    assertEquals(9, opts.reducers);
    assertEquals(8, opts.fanout);
    assertEquals(7, opts.maxSegments);
    assertEquals(new Integer(1), opts.shards);
    assertEquals(null, opts.fairSchedulerPool);
    assertTrue(opts.isVerbose);
    assertEquals(Arrays.asList(new Path("file:///home"), new Path("file:///dev")), opts.inputFiles);
    assertEquals(RetainMostRecentUpdateConflictResolver.class.getName(), opts.updateConflictResolver);
    assertEquals(MORPHLINE_FILE, opts.morphlineFile.getPath());
    assertEquals("morphline_xyz", opts.morphlineId);
    assertEmptySystemErrAndEmptySystemOut();
  }

  @Test
  public void testArgsParserMultipleSpecsOfSameKind() {
    String[] args = new String[] { 
        "--input-list", "file:///tmp",
        "--input-list", "file:///",
        "--morphline-file", MORPHLINE_FILE,
        "--output-dir", "file:/tmp/foo",
        "--solr-home-dir", SOLR_HOME_DIR, 
        "--shards", "1",
        "file:///home",
        "file:///dev",
        };
    assertNull(parser.parseArgs(args, conf, opts));
    assertEquals(Arrays.asList(new Path("file:///tmp"), new Path("file:///")), opts.inputLists);
    assertEquals(Arrays.asList(new Path("file:///home"), new Path("file:///dev")), opts.inputFiles);
    assertEquals(new Path("file:/tmp/foo"), opts.outputDir);
    assertEquals(new File(SOLR_HOME_DIR), opts.solrHomeDir);
    assertEmptySystemErrAndEmptySystemOut();
  }

  @Test
  public void testArgsParserTypicalUseWithEqualsSign() {
    String[] args = new String[] { 
        "--input-list=file:///tmp",
        "--morphline-file", MORPHLINE_FILE,
        "--output-dir=file:/tmp/foo",
        "--solr-home-dir=" + SOLR_HOME_DIR, 
        "--mappers=10", 
        "--shards", "1",
        "--verbose", 
        "file:///home",
        "file:///dev",
        };
    assertNull(parser.parseArgs(args, conf, opts));
    assertEquals(Collections.singletonList(new Path("file:///tmp")), opts.inputLists);
    assertEquals(new Path("file:/tmp/foo"), opts.outputDir);
    assertEquals(new File(SOLR_HOME_DIR), opts.solrHomeDir);
    assertEquals(10, opts.mappers);
    assertEquals(new Integer(1), opts.shards);
    assertEquals(null, opts.fairSchedulerPool);
    assertTrue(opts.isVerbose);
    assertEquals(Arrays.asList(new Path("file:///home"), new Path("file:///dev")), opts.inputFiles);
    assertEmptySystemErrAndEmptySystemOut();
  }

  @Test
  public void testArgsParserMultipleSpecsOfSameKindWithEqualsSign() {
    String[] args = new String[] { 
        "--input-list=file:///tmp",
        "--input-list=file:///",
        "--morphline-file", MORPHLINE_FILE,
        "--output-dir=file:/tmp/foo",
        "--solr-home-dir=" + SOLR_HOME_DIR, 
        "--shards", "1",
        "file:///home",
        "file:///dev",
        };
    assertNull(parser.parseArgs(args, conf, opts));
    assertEquals(Arrays.asList(new Path("file:///tmp"), new Path("file:///")), opts.inputLists);
    assertEquals(Arrays.asList(new Path("file:///home"), new Path("file:///dev")), opts.inputFiles);
    assertEquals(new Path("file:/tmp/foo"), opts.outputDir);
    assertEquals(new File(SOLR_HOME_DIR), opts.solrHomeDir);
    assertEmptySystemErrAndEmptySystemOut();
  }

  @Test
  public void testArgsParserHelp() throws UnsupportedEncodingException  {
    String[] args = new String[] { "--help" };
    assertEquals(new Integer(0), parser.parseArgs(args, conf, opts));
    String helpText = new String(bout.toByteArray(), "UTF-8");
    assertTrue(helpText.contains("MapReduce batch job driver that creates "));
    assertTrue(helpText.contains("bin/hadoop command"));
    assertEquals(0, berr.toByteArray().length);
  }
  
  @Test
  public void testArgsParserOk() {
    String[] args = new String[] { 
        "--input-list", "file:///tmp",
        "--morphline-file", MORPHLINE_FILE,
        "--output-dir", "file:/tmp/foo",
        "--solr-home-dir", SOLR_HOME_DIR, 
        "--shards", "1",
        };
    assertNull(parser.parseArgs(args, conf, opts));
    assertEquals(new Integer(1), opts.shards);
    assertEmptySystemErrAndEmptySystemOut();
  }

  @Test
  public void testArgsParserUpdateConflictResolver() {
    String[] args = new String[] { 
        "--input-list", "file:///tmp",
        "--morphline-file", MORPHLINE_FILE,
        "--output-dir", "file:/tmp/foo",
        "--solr-home-dir", SOLR_HOME_DIR, 
        "--shards", "1",
        "--update-conflict-resolver", NoChangeUpdateConflictResolver.class.getName(),
        };
    assertNull(parser.parseArgs(args, conf, opts));
    assertEquals(NoChangeUpdateConflictResolver.class.getName(), opts.updateConflictResolver);
    assertEmptySystemErrAndEmptySystemOut();
  }

  @Test
  public void testArgsParserUnknownArgName() {
    String[] args = new String[] { 
        "--xxxxxxxxinputlist", "file:///tmp",
        "--morphline-file", MORPHLINE_FILE,
        "--output-dir", "file:/tmp/foo",
        "--solr-home-dir", SOLR_HOME_DIR, 
        "--shards", "1",
        };
    assertArgumentParserException(args);
  }

  @Test
  public void testArgsParserFileNotFound1() {
    String[] args = new String[] { 
        "--input-list", "file:///tmp",
        "--morphline-file", MORPHLINE_FILE,
        "--output-dir", "file:/fileNotFound/foo",
        "--solr-home-dir", SOLR_HOME_DIR, 
        "--shards", "1",
        };
    assertArgumentParserException(args);
  }
  
  @Test
  public void testArgsParserFileNotFound2() {
    String[] args = new String[] { 
        "--input-list", "file:///tmp",
        "--morphline-file", MORPHLINE_FILE,
        "--output-dir", "file:/tmp/foo",
        "--solr-home-dir", "/fileNotFound", 
        "--shards", "1",
        };
    assertArgumentParserException(args);
  }
  
  @Test
  public void testArgsParserIntOutOfRange() {
    String[] args = new String[] { 
        "--input-list", "file:///tmp",
        "--morphline-file", MORPHLINE_FILE,
        "--output-dir", "file:/tmp/foo",
        "--solr-home-dir", SOLR_HOME_DIR, 
        "--shards", "1",
        "--mappers", "-20"
        };
    assertArgumentParserException(args);
  }
  
  @Test
  public void testArgsParserIllegalFanout() {
    String[] args = new String[] { 
        "--input-list", "file:///tmp",
        "--morphline-file", MORPHLINE_FILE,
        "--output-dir", "file:/tmp/foo",
        "--solr-home-dir", SOLR_HOME_DIR, 
        "--shards", "1",
        "--fanout", "1" // must be >= 2
        };
    assertArgumentParserException(args);
  }
  
  @Test
  public void testArgsParserSolrHomeMustContainSolrConfigFile() {
    String[] args = new String[] { 
        "--input-list", "file:///tmp",
        "--morphline-file", MORPHLINE_FILE,
        "--output-dir", "file:/tmp/foo",
        "--shards", "1",
        "--solr-home-dir", "/",
        };
    assertArgumentParserException(args);
  }

  @Test
  public void testArgsShardUrlOk() {
    String[] args = new String[] { 
        "--input-list", "file:///tmp",
        "--morphline-file", MORPHLINE_FILE,
        "--output-dir", "file:/tmp/foo",
        "--solr-home-dir", SOLR_HOME_DIR, 
        "--shard-url", "http://localhost:8983/solr/collection1",
        "--shard-url", "http://localhost:8983/solr/collection2",
        };
    assertNull(parser.parseArgs(args, conf, opts));
    assertEquals(Arrays.asList(
        Collections.singletonList("http://localhost:8983/solr/collection1"),
        Collections.singletonList("http://localhost:8983/solr/collection2")),
        opts.shardUrls);
    assertEquals(new Integer(2), opts.shards);
    assertEmptySystemErrAndEmptySystemOut();
  }
  
  @Test
  public void testArgsShardUrlMustHaveAParam() {
    String[] args = new String[] { 
        "--input-list", "file:///tmp",
        "--morphline-file", MORPHLINE_FILE,
        "--output-dir", "file:/tmp/foo",
        "--solr-home-dir", SOLR_HOME_DIR, 
        "--shard-url",
        };
    assertArgumentParserException(args);
  }
  
  @Test
  public void testArgsShardUrlAndShardsSucceeds() {
    String[] args = new String[] { 
        "--input-list", "file:///tmp",
        "--morphline-file", MORPHLINE_FILE,
        "--output-dir", "file:/tmp/foo",
        "--solr-home-dir", SOLR_HOME_DIR, 
        "--shards", "1", 
        "--shard-url", "http://localhost:8983/solr/collection1",
        };
    assertNull(parser.parseArgs(args, conf, opts));
    assertEmptySystemErrAndEmptySystemOut();
  }
  
  @Test
  public void testArgsShardUrlNoGoLive() {
    String[] args = new String[] { 
        "--input-list", "file:///tmp",
        "--morphline-file", MORPHLINE_FILE,
        "--output-dir", "file:/tmp/foo",
        "--solr-home-dir", SOLR_HOME_DIR, 
        "--shard-url", "http://localhost:8983/solr/collection1"
        };
    assertNull(parser.parseArgs(args, conf, opts));
    assertEmptySystemErrAndEmptySystemOut();
    assertEquals(new Integer(1), opts.shards);
  }
  
  @Test
  public void testArgsShardUrlsAndZkhostAreMutuallyExclusive() {
    String[] args = new String[] { 
        "--input-list", "file:///tmp",
        "--morphline-file", MORPHLINE_FILE,
        "--output-dir", "file:/tmp/foo",
        "--solr-home-dir", SOLR_HOME_DIR, 
        "--shard-url", "http://localhost:8983/solr/collection1",
        "--shard-url", "http://localhost:8983/solr/collection1",
        "--zk-host", "http://localhost:2185",
        "--go-live"
        };
    assertArgumentParserException(args);
  }
  
  @Test
  public void testArgsGoLiveAndSolrUrl() {
    String[] args = new String[] { 
        "--input-list", "file:///tmp",
        "--morphline-file", MORPHLINE_FILE,
        "--output-dir", "file:/tmp/foo",
        "--solr-home-dir", SOLR_HOME_DIR, 
        "--shard-url", "http://localhost:8983/solr/collection1",
        "--shard-url", "http://localhost:8983/solr/collection1",
        "--go-live"
        };
    Integer result = parser.parseArgs(args, conf, opts);
    assertNull(result);
    assertEmptySystemErrAndEmptySystemOut();
  }
  
  @Test
  public void testArgsZkHostNoGoLive() {
    String[] args = new String[] { 
        "--input-list", "file:///tmp",
        "--morphline-file", MORPHLINE_FILE,
        "--output-dir", "file:/tmp/foo",
        "--solr-home-dir", SOLR_HOME_DIR, 
        "--zk-host", "http://localhost:2185",
        };
    assertArgumentParserException(args);
  }
  
  @Test
  public void testArgsGoLiveZkHostNoCollection() {
    String[] args = new String[] { 
        "--input-list", "file:///tmp",
        "--morphline-file", MORPHLINE_FILE,
        "--output-dir", "file:/tmp/foo",
        "--solr-home-dir", SOLR_HOME_DIR, 
        "--zk-host", "http://localhost:2185",
        "--go-live"
        };
    assertArgumentParserException(args);
  }
  
  @Test
  public void testArgsGoLiveNoZkHostOrSolrUrl() {
    String[] args = new String[] { 
        "--input-list", "file:///tmp",
        "--morphline-file", MORPHLINE_FILE,
        "--output-dir", "file:/tmp/foo",
        "--solr-home-dir", SOLR_HOME_DIR, 
        "--go-live"
        };
    assertArgumentParserException(args);
  }  

  @Test
  public void testNoSolrHomeDirOrZKHost() {
    String[] args = new String[] {
        "--input-list", "file:///tmp",
        "--morphline-file", MORPHLINE_FILE,
        "--output-dir", "file:/tmp/foo",
        "--shards", "1",
        };
    assertArgumentParserException(args);
  }

  @Test
  public void testZKHostNoSolrHomeDirOk() {
    String[] args = new String[] {
        "--input-list", "file:///tmp",
        "--morphline-file", MORPHLINE_FILE,
        "--output-dir", "file:/tmp/foo",
        "--zk-host", "http://localhost:2185",
        "--collection", "collection1",
        };
    assertNull(parser.parseArgs(args, conf, opts));
    assertEmptySystemErrAndEmptySystemOut();
  }

  private void assertEmptySystemErrAndEmptySystemOut() {
    assertEquals(0, bout.toByteArray().length);
    assertEquals(0, berr.toByteArray().length);
  }
  
  private void assertArgumentParserException(String[] args) {
    assertEquals("should have returned fail code", new Integer(1), parser.parseArgs(args, conf, opts));
    assertEquals("no sys out expected:" + new String(bout.toByteArray()), 0, bout.toByteArray().length);
    String usageText;
    try {
      usageText = new String(berr.toByteArray(), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("unreachable");
    }
    
    assertTrue("should start with usage msg:" + usageText, usageText.startsWith("usage: hadoop "));
  }
  
}
