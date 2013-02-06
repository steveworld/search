/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.shell.find;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.fs.shell.find.Blocksize;
import org.apache.hadoop.fs.shell.find.FindOptions;
import org.apache.hadoop.fs.shell.find.Result;
import org.junit.Test;
import org.junit.Before;

public class TestBlocksize extends TestExpression {
  private MockFileSystem fs;
  private PathData one;
  private PathData two;
  private PathData three;
  private PathData four;
  private PathData five;
  
  @Before
  public void setUp() throws IOException {
    MockFileSystem.reset();
    fs = new MockFileSystem();
    
    FileStatus fileStatus;
    
    fileStatus = mock(FileStatus.class);
    when(fileStatus.getBlockSize()).thenReturn(1l);
    fs.setFileStatus("one", fileStatus);
    one = new PathData("one", fs.getConf());

    fileStatus = mock(FileStatus.class);
    when(fileStatus.getBlockSize()).thenReturn(2l);
    fs.setFileStatus("two", fileStatus);
    two = new PathData("two", fs.getConf());

    fileStatus = mock(FileStatus.class);
    when(fileStatus.getBlockSize()).thenReturn(3l);
    fs.setFileStatus("three", fileStatus);
    three = new PathData("three", fs.getConf());

    fileStatus = mock(FileStatus.class);
    when(fileStatus.getBlockSize()).thenReturn(4l);
    fs.setFileStatus("four", fileStatus);
    four = new PathData("four", fs.getConf());

    fileStatus = mock(FileStatus.class);
    when(fileStatus.getBlockSize()).thenReturn(5l);
    fs.setFileStatus("five", fileStatus);
    five = new PathData("five", fs.getConf());
  }

  @Test
  public void applyEquals() throws IOException {
    Blocksize blocksize = new Blocksize();
    addArgument(blocksize, "3");
    blocksize.initialise(new FindOptions());

    assertEquals(Result.FAIL, blocksize.apply(one));
    assertEquals(Result.FAIL, blocksize.apply(two));
    assertEquals(Result.PASS, blocksize.apply(three));
    assertEquals(Result.FAIL, blocksize.apply(four));
    assertEquals(Result.FAIL, blocksize.apply(five));
  }

  @Test
  public void applyGreaterThan() throws IOException {
    Blocksize blocksize = new Blocksize();
    addArgument(blocksize, "+3");
    blocksize.initialise(new FindOptions());

    assertEquals(Result.FAIL, blocksize.apply(one));
    assertEquals(Result.FAIL, blocksize.apply(two));
    assertEquals(Result.FAIL, blocksize.apply(three));
    assertEquals(Result.PASS, blocksize.apply(four));
    assertEquals(Result.PASS, blocksize.apply(five));
  }

  @Test
  public void applyLessThan() throws IOException {
    Blocksize blocksize = new Blocksize();
    addArgument(blocksize, "-3");
    blocksize.initialise(new FindOptions());

    assertEquals(Result.PASS, blocksize.apply(one));
    assertEquals(Result.PASS, blocksize.apply(two));
    assertEquals(Result.FAIL, blocksize.apply(three));
    assertEquals(Result.FAIL, blocksize.apply(four));
    assertEquals(Result.FAIL, blocksize.apply(five));
  }
}
