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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TikaIndexerToolTest extends Assert {

  private FileSystem fs; 
  private TikaIndexerTool.MyArgumentParser parser;
  
  @Before
  public void setUp() throws IOException {
    fs = FileSystem.get(new Configuration());
    parser = new TikaIndexerTool.MyArgumentParser();
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
    assertNull(parser.parseArgs(args, fs));
    assertEquals(Collections.singletonList(new Path("file:///tmp")), parser.inputLists);
    assertEquals(new Path("file:/tmp/foo"), parser.outputDir);
    assertEquals(new File("/"), parser.solrHomeDir);
    assertEquals(10, parser.mappers);
    assertEquals(1, parser.shards);
    assertEquals(null, parser.fairSchedulerPool);
    assertTrue(parser.isVerbose);
    assertTrue(parser.isRandomize);
    assertFalse(parser.isIdentityTest);
    assertEquals(Arrays.asList(new Path("file:///home"), new Path("file:///dev")), parser.inputFiles);
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
    assertNull(parser.parseArgs(args, fs));
    assertEquals(Arrays.asList(new Path("file:///tmp"), new Path("file:///")), parser.inputLists);
    assertEquals(Arrays.asList(new Path("file:///home"), new Path("file:///dev")), parser.inputFiles);
    assertEquals(new Path("file:/tmp/foo"), parser.outputDir);
    assertEquals(new File("/"), parser.solrHomeDir);
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
    assertNull(parser.parseArgs(args, fs));
    assertEquals(Collections.singletonList(new Path("file:///tmp")), parser.inputLists);
    assertEquals(new Path("file:/tmp/foo"), parser.outputDir);
    assertEquals(new File("/"), parser.solrHomeDir);
    assertEquals(10, parser.mappers);
    assertEquals(1, parser.shards);
    assertEquals(null, parser.fairSchedulerPool);
    assertTrue(parser.isVerbose);
    assertTrue(parser.isRandomize);
    assertFalse(parser.isIdentityTest);
    assertEquals(Arrays.asList(new Path("file:///home"), new Path("file:///dev")), parser.inputFiles);
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
    assertNull(parser.parseArgs(args, fs));
    assertEquals(Arrays.asList(new Path("file:///tmp"), new Path("file:///")), parser.inputLists);
    assertEquals(Arrays.asList(new Path("file:///home"), new Path("file:///dev")), parser.inputFiles);
    assertEquals(new Path("file:/tmp/foo"), parser.outputDir);
    assertEquals(new File("/"), parser.solrHomeDir);
  }

  @Test
  public void testArgsParserHelp() throws UnsupportedEncodingException  {
    PrintStream oldSystemOut = System.out;
    try {
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      System.setOut(new PrintStream(bout, true, "UTF-8"));
      String[] args = new String[] { "--help" };
      assertEquals(new Integer(0), parser.parseArgs(args, fs));
      String helpText = new String(bout.toByteArray(), "UTF-8");
      assertTrue(helpText.contains("Map Reduce job that creates a Solr index from a set of input files"));
    } finally {
      System.setOut(oldSystemOut);
    }
  }
  
}
