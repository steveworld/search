package org.apache.hadoop.fs.shell.find;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.fs.shell.find.FindOptions;
import org.apache.hadoop.fs.shell.find.Mtime;
import org.apache.hadoop.fs.shell.find.Result;
import org.junit.Before;
import org.junit.Test;

public class TestMmin extends TestExpression {
  private static final long MIN = 60l * 1000l;
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
    when(fourDaysStat.getModificationTime()).thenReturn(NOW - (4l * MIN));
    when(fourDaysStat.toString()).thenReturn("fourDays");
    fs.setFileStatus("fourDays", fourDaysStat);
    fourDays = new PathData("fourDays", conf);

    FileStatus fiveDaysStat = mock(FileStatus.class);
    when(fiveDaysStat.getModificationTime()).thenReturn(NOW - (5l * MIN));
    when(fiveDaysStat.toString()).thenReturn("fiveDays");
    fs.setFileStatus("fiveDays", fiveDaysStat);
    fiveDays = new PathData("fiveDays", conf);

    FileStatus fiveDaysMinus1Stat = mock(FileStatus.class);
    when(fiveDaysMinus1Stat.getModificationTime()).thenReturn(NOW - ((5l * MIN) - 1));
    when(fiveDaysMinus1Stat.toString()).thenReturn("fiveDaysMinus");
    fs.setFileStatus("fiveDaysMinus", fiveDaysMinus1Stat);
    fiveDaysMinus = new PathData("fiveDaysMinus", conf);

    FileStatus sixDaysStat = mock(FileStatus.class);
    when(sixDaysStat.getModificationTime()).thenReturn(NOW - (6l * MIN));
    when(sixDaysStat.toString()).thenReturn("sixDays");
    fs.setFileStatus("sixDays", sixDaysStat);
    sixDays = new PathData("sixDays", conf);

    FileStatus sixDaysMinus1Stat = mock(FileStatus.class);
    when(sixDaysMinus1Stat.getModificationTime()).thenReturn(NOW - ((6l * MIN) - 1));
    when(sixDaysMinus1Stat.toString()).thenReturn("fiveDaysPlus");
    fs.setFileStatus("fiveDaysPlus", sixDaysMinus1Stat);
    fiveDaysPlus = new PathData("fiveDaysPlus", conf);
  }

  @Test
  public void testExact() throws IOException {
    Mtime.Mmin mmin = new Mtime.Mmin();
    addArgument(mmin, "5");
    
    FindOptions options = new FindOptions();
    options.setStartTime(NOW);
    mmin.initialise(options);
    
    assertEquals(Result.FAIL, mmin.apply(fourDays));
    assertEquals(Result.FAIL, mmin.apply(fiveDaysMinus));
    assertEquals(Result.PASS, mmin.apply(fiveDays));
    assertEquals(Result.PASS, mmin.apply(fiveDaysPlus));
    assertEquals(Result.FAIL, mmin.apply(sixDays));
  }
  
  @Test
  public void testGreater() throws IOException {
    Mtime.Mmin mmin = new Mtime.Mmin();
    addArgument(mmin, "+5");

    FindOptions options = new FindOptions();
    options.setStartTime(NOW);
    mmin.initialise(options);
 
    assertEquals(Result.FAIL, mmin.apply(fourDays));
    assertEquals(Result.FAIL, mmin.apply(fiveDaysMinus));
    assertEquals(Result.FAIL, mmin.apply(fiveDays));
    assertEquals(Result.FAIL, mmin.apply(fiveDaysPlus));
    assertEquals(Result.PASS, mmin.apply(sixDays));
  }

  @Test
  public void testLess() throws IOException {
    Mtime.Mmin mmin = new Mtime.Mmin();
    addArgument(mmin, "-5");
    
    FindOptions options = new FindOptions();
    options.setStartTime(NOW);
    mmin.initialise(options);
 
    assertEquals(Result.PASS, mmin.apply(fourDays));
    assertEquals(Result.PASS, mmin.apply(fiveDaysMinus));
    assertEquals(Result.FAIL, mmin.apply(fiveDays));
    assertEquals(Result.FAIL, mmin.apply(fiveDaysPlus));
    assertEquals(Result.FAIL, mmin.apply(sixDays));
  }

}
