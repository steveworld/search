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
package org.apache.solr.hadoop.fs.shell.find;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.solr.hadoop.fs.shell.find.FindOptions;
import org.apache.solr.hadoop.fs.shell.find.Group;
import org.apache.solr.hadoop.fs.shell.find.Result;
import org.junit.Before;
import org.junit.Test;

public class TestGroup extends TestExpression {
  private MockFileSystem fs;
  private Configuration conf;
  private FileStatus fileStatus;
  private PathData item;
  private Group group;

  @Before
  public void setUp() throws IOException {
    MockFileSystem.reset();
    fs = new MockFileSystem();
    conf = fs.getConf();
    fileStatus = mock(FileStatus.class);
    fs.setFileStatus("test", fileStatus);
    item = new PathData("/one/two/test", conf);

    group = new Group();
    addArgument(group, "group");
    group.initialise(new FindOptions());
  }
  
  @Test
  public void applyPass() throws IOException{
    when(fileStatus.getGroup()).thenReturn("group");
    
    assertEquals(Result.PASS, group.apply(item));
  }
  
  @Test
  public void applyFail() throws IOException {
    when(fileStatus.getGroup()).thenReturn("notgroup");
    
    assertEquals(Result.FAIL, group.apply(item));
  }

  @Test
  public void applyBlank() throws IOException {
    when(fileStatus.getGroup()).thenReturn("");
    
    assertEquals(Result.FAIL, group.apply(item));
  }

  @Test
  public void applyNull() throws IOException {
    when(fileStatus.getGroup()).thenReturn(null);
    
    assertEquals(Result.FAIL, group.apply(item));
  }
}
