package org.apache.hadoop.fs.shell.find;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.fs.shell.find.Empty;
import org.apache.hadoop.fs.shell.find.FindOptions;
import org.apache.hadoop.fs.shell.find.Result;
import org.junit.Test;
import org.junit.Before;

public class TestEmpty extends TestExpression {
  private MockFileSystem fs;
  
  @Before
  public void setUp() throws IOException {
    MockFileSystem.reset();
    fs = new MockFileSystem();
  }

  @Test
  public void applyEmptyFile() throws IOException {
    FileStatus fileStatus = mock(FileStatus.class);
    when(fileStatus.isDirectory()).thenReturn(false);
    when(fileStatus.getLen()).thenReturn(0l);
    fs.setFileStatus("emptyFile", fileStatus);
    PathData item = new PathData("emptyFile", fs.getConf());
    
    Empty empty = new Empty();
    empty.initialise(new FindOptions());

    assertEquals(Result.PASS, empty.apply(item));
  }
  
  @Test
  public void applyNotEmptyFile() throws IOException {
    FileStatus fileStatus = mock(FileStatus.class);
    when(fileStatus.isDirectory()).thenReturn(false);
    when(fileStatus.getLen()).thenReturn(1l);
    fs.setFileStatus("notEmptyFile", fileStatus);
    PathData item = new PathData("notEmptyFile", fs.getConf());
    
    Empty empty = new Empty();
    empty.initialise(new FindOptions());

    assertEquals(Result.FAIL, empty.apply(item));
  }

  @Test
  public void applyEmptyDirectory() throws IOException {
    FileStatus fileStatus = mock(FileStatus.class);
    when(fileStatus.isDirectory()).thenReturn(false);
    fs.setFileStatus("emptyDirectory", fileStatus);
    fs.setListStatus("emptyDirectory", new FileStatus[0]);
    PathData item = new PathData("emptyDirectory", fs.getConf());
    
    Empty empty = new Empty();
    empty.initialise(new FindOptions());

    assertEquals(Result.PASS, empty.apply(item));
  }

  @Test
  public void applyNotEmptyDirectory() throws IOException {
    FileStatus fileStatus = mock(FileStatus.class);
    when(fileStatus.isDirectory()).thenReturn(false);
    fs.setFileStatus("notEmptyDirectory", fileStatus);
    fs.setListStatus("notEmptyDirectory", new FileStatus[] {mock(FileStatus.class)});
    PathData item = new PathData("notEmptyDirectory", fs.getConf());
    
    Empty empty = new Empty();
    empty.initialise(new FindOptions());

    assertEquals(Result.PASS, empty.apply(item));
  }
}
