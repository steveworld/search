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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TikaIndexerToolArgumentParserTest extends Assert {

  private FileSystem fs; 
  private TikaIndexerTool.MyArgumentParser parser;
  private TikaIndexerTool.Options opts;
  private PrintStream oldSystemOut;
  private PrintStream oldSystemErr;
  private ByteArrayOutputStream bout;
  private ByteArrayOutputStream berr;
  
  @Before
  public void setUp() throws IOException {
    fs = FileSystem.get(new Configuration());
    parser = new TikaIndexerTool.MyArgumentParser();
    opts = new TikaIndexerTool.Options();
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
        "--verbose", 
        "file:///home",
        "file:///dev",
        };
    assertNull(parser.parseArgs(args, fs, opts));
    assertEquals(Collections.singletonList(new Path("file:///tmp")), opts.inputLists);
    assertEquals(new Path("file:/tmp/foo"), opts.outputDir);
    assertEquals(new File("/"), opts.solrHomeDir);
    assertEquals(10, opts.mappers);
    assertEquals(1, opts.shards);
    assertEquals(null, opts.fairSchedulerPool);
    assertTrue(opts.isVerbose);
    assertTrue(opts.isRandomize);
    assertFalse(opts.isIdentityTest);
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
        "--verbose", 
        "file:///home",
        "file:///dev",
        };
    assertNull(parser.parseArgs(args, fs, opts));
    assertEquals(Collections.singletonList(new Path("file:///tmp")), opts.inputLists);
    assertEquals(new Path("file:/tmp/foo"), opts.outputDir);
    assertEquals(new File("/"), opts.solrHomeDir);
    assertEquals(10, opts.mappers);
    assertEquals(1, opts.shards);
    assertEquals(null, opts.fairSchedulerPool);
    assertTrue(opts.isVerbose);
    assertTrue(opts.isRandomize);
    assertFalse(opts.isIdentityTest);
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
    assertTrue(helpText.contains("Map Reduce job that creates a set of Solr index shards"));
    assertEquals(0, berr.toByteArray().length);
  }
  
  @Test
  public void testArgsParserOk() {
    String[] args = new String[] { 
        "--inputlist", "file:///tmp",
        "--outputdir", "file:/tmp/foo",
        "--solrhomedir", "/", 
        };
    assertNull(parser.parseArgs(args, fs, opts));
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
  
  private void assertEmptySystemErrAndEmptySystemOut() {
    assertEquals(0, bout.toByteArray().length);
    assertEquals(0, berr.toByteArray().length);
  }
  
  private void assertArgumentParserException(String[] args) {
    assertEquals(new Integer(1), parser.parseArgs(args, fs, opts));
    assertEquals(0, bout.toByteArray().length);
    String usageText;
    try {
      usageText = new String(berr.toByteArray(), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException("unreachable");
    }
    assertTrue(usageText.startsWith("usage: hadoop "));
  }
  
}
