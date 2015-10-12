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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.solr.hadoop.fs.shell.find.FindOptions;
import org.apache.solr.hadoop.fs.shell.find.Name;
import org.apache.solr.hadoop.fs.shell.find.Result;
import org.junit.Before;
import org.junit.Test;

public class TestName extends TestExpression {
  private static FileSystem fs;
  private static Configuration conf;
  private Name name;

  @Before
  public void setUp() throws IOException {
    MockFileSystem.reset();
    fs = new MockFileSystem();
    conf = fs.getConf();

    name = new Name();
    addArgument(name, "name");
    name.initialise(new FindOptions());
  }
  
  @Test
  public void applyPass() throws IOException{
    PathData item = new PathData("/directory/path/name", conf);
    assertEquals(Result.PASS, name.apply(item));
  }

  @Test
  public void applyFail() throws IOException{
    PathData item = new PathData("/directory/path/notname", conf);
    assertEquals(Result.FAIL, name.apply(item));
  }
  @Test
  public void applyMixedCase() throws IOException{
    PathData item = new PathData("/directory/path/NaMe", conf);
    assertEquals(Result.FAIL, name.apply(item));
  }
}
