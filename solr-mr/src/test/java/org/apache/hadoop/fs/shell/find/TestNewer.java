package org.apache.hadoop.fs.shell.find;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.fs.shell.find.FindOptions;
import org.apache.hadoop.fs.shell.find.Newer;
import org.apache.hadoop.fs.shell.find.Result;
import org.junit.Before;
import org.junit.Test;

public class TestNewer extends TestExpression {
  private static final long NOW = new Date().getTime();
  private static MockFileSystem fs;
  private Newer newer;

  @Before
  public void setUp() throws IOException {
    MockFileSystem.reset();
    fs = new MockFileSystem();
    
    FileStatus fileStatus = mock(FileStatus.class);
    when(fileStatus.getModificationTime()).thenReturn(NOW - (5l*86400000));
    fs.setFileStatus("comparison.file", fileStatus);

    newer = new Newer();
    newer.setConf(fs.getConf());
    addArgument(newer, "comparison.file");
    newer.initialise(new FindOptions());
  }
  
  @Test
  public void applyPass() throws IOException{
    FileStatus fileStatus = mock(FileStatus.class);
    when(fileStatus.getModificationTime()).thenReturn(NOW - (4l*86400000));
    fs.setFileStatus("newer.file", fileStatus);
    PathData item = new PathData("/directory/path/newer.file", fs.getConf());
    assertEquals(Result.PASS, newer.apply(item));
  }

  @Test
  public void applyFail() throws IOException{
    FileStatus fileStatus = mock(FileStatus.class);
    when(fileStatus.getModificationTime()).thenReturn(NOW - (6l*86400000));
    fs.setFileStatus("older.file", fileStatus);
    PathData item = new PathData("/directory/path/older.file", fs.getConf());
    assertEquals(Result.FAIL, newer.apply(item));
  }
}
