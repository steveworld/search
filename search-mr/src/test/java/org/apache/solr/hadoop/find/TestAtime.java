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
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.solr.hadoop.fs.shell.find.Atime;
import org.apache.solr.hadoop.fs.shell.find.FindOptions;
import org.apache.solr.hadoop.fs.shell.find.Result;
import org.junit.Before;
import org.junit.Test;

public class TestAtime extends TestExpression {
  private static final long DAY = 86400l * 1000l;
  private final long NOW = new Date().getTime();
  
  private MockFileSystem fs;
  private Configuration conf;
  
  private PathData fourDays;
  private PathData fiveDays;
  private PathData fiveDaysMinus;
  private PathData sixDays;
  private PathData fiveDaysPlus;

  @Before
  public void setup() throws IOException {
    MockFileSystem.reset();
    fs = new MockFileSystem();
    conf = fs.getConf();
    
    FileStatus fourDaysStat = mock(FileStatus.class);
    when(fourDaysStat.getAccessTime()).thenReturn(NOW - (4l * DAY));
    when(fourDaysStat.toString()).thenReturn("fourDays");
    fs.setFileStatus("fourDays", fourDaysStat);
    fourDays = new PathData("fourDays", conf);

    FileStatus fiveDaysStat = mock(FileStatus.class);
    when(fiveDaysStat.getAccessTime()).thenReturn(NOW - (5l * DAY));
    when(fiveDaysStat.toString()).thenReturn("fiveDays");
    fs.setFileStatus("fiveDays", fiveDaysStat);
    fiveDays = new PathData("fiveDays", conf);

    FileStatus fiveDaysMinus1Stat = mock(FileStatus.class);
    when(fiveDaysMinus1Stat.getAccessTime()).thenReturn(NOW - ((5l * DAY) - 1));
    when(fiveDaysMinus1Stat.toString()).thenReturn("fiveDaysMinus");
    fs.setFileStatus("fiveDaysMinus", fiveDaysMinus1Stat);
    fiveDaysMinus = new PathData("fiveDaysMinus", conf);

    FileStatus sixDaysStat = mock(FileStatus.class);
    when(sixDaysStat.getAccessTime()).thenReturn(NOW - (6l * DAY));
    when(sixDaysStat.toString()).thenReturn("sixDays");
    fs.setFileStatus("sixDays", sixDaysStat);
    sixDays = new PathData("sixDays", conf);

    FileStatus sixDaysMinus1Stat = mock(FileStatus.class);
    when(sixDaysMinus1Stat.getAccessTime()).thenReturn(NOW - ((6l * DAY) - 1));
    when(sixDaysMinus1Stat.toString()).thenReturn("fiveDaysPlus");
    fs.setFileStatus("fiveDaysPlus", sixDaysMinus1Stat);
    fiveDaysPlus = new PathData("fiveDaysPlus", conf);

  }

  @Test
  public void testExact() throws IOException {
    Atime atime = new Atime();
    addArgument(atime, "5");
    
    FindOptions options = new FindOptions();
    options.setStartTime(NOW);
    atime.initialise(options);
    
    assertEquals(Result.FAIL, atime.apply(fourDays));
    assertEquals(Result.FAIL, atime.apply(fiveDaysMinus));
    assertEquals(Result.PASS, atime.apply(fiveDays));
    assertEquals(Result.PASS, atime.apply(fiveDaysPlus));
    assertEquals(Result.FAIL, atime.apply(sixDays));
  }
  
  @Test
  public void testGreater() throws IOException {
    Atime atime = new Atime();
    addArgument(atime, "+5");

    FindOptions options = new FindOptions();
    options.setStartTime(NOW);
    atime.initialise(options);
 
    assertEquals(Result.FAIL, atime.apply(fourDays));
    assertEquals(Result.FAIL, atime.apply(fiveDaysMinus));
    assertEquals(Result.FAIL, atime.apply(fiveDays));
    assertEquals(Result.FAIL, atime.apply(fiveDaysPlus));
    assertEquals(Result.PASS, atime.apply(sixDays));
  }

  @Test
  public void testLess() throws IOException {
    Atime atime = new Atime();
    addArgument(atime, "-5");
    
    FindOptions options = new FindOptions();
    options.setStartTime(NOW);
    atime.initialise(options);
 
    assertEquals(Result.PASS, atime.apply(fourDays));
    assertEquals(Result.PASS, atime.apply(fiveDaysMinus));
    assertEquals(Result.FAIL, atime.apply(fiveDays));
    assertEquals(Result.FAIL, atime.apply(fiveDaysPlus));
    assertEquals(Result.FAIL, atime.apply(sixDays));
  }
}
