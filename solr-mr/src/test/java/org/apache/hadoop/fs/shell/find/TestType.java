package org.apache.hadoop.fs.shell.find;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.fs.shell.find.FindOptions;
import org.apache.hadoop.fs.shell.find.Result;
import org.apache.hadoop.fs.shell.find.Type;
import org.junit.Before;
import org.junit.Test;

public class TestType extends TestExpression {
  private MockFileSystem fs;
  private Configuration conf;
  private FileStatus mockFstat;

  @Before
  public void setup() throws IOException {
    MockFileSystem.reset();
    fs = new MockFileSystem();
    conf = fs.getConf();
    mockFstat = mock(FileStatus.class);
    when(mockFstat.isDirectory()).thenReturn(false);
    when(mockFstat.isSymlink()).thenReturn(false);
    when(mockFstat.isFile()).thenReturn(false);
    fs.setFileStatus("test", mockFstat);
  }

  @Test
  public void testIsDirectory() throws IOException{
    Type type = new Type();
    addArgument(type, "d");
    type.initialise(new FindOptions());
    
    when(mockFstat.isDirectory()).thenReturn(true);
    PathData item = new PathData("/one/two/test", conf);

    assertEquals(Result.PASS, type.apply(item));
  }

  @Test
  public void testIsNotDirectory() throws IOException{
    Type type = new Type();
    addArgument(type, "d");
    type.initialise(new FindOptions());
    
    when(mockFstat.isDirectory()).thenReturn(false);
    PathData item = new PathData("/one/two/test", conf);

    assertEquals(Result.FAIL, type.apply(item));
  }

  @Test
  public void testIsSymlink() throws IOException{
    Type type = new Type();
    addArgument(type, "l");
    type.initialise(new FindOptions());
    
    when(mockFstat.isSymlink()).thenReturn(true);
    PathData item = new PathData("/one/two/test", conf);

    assertEquals(Result.PASS, type.apply(item));
  }

  @Test
  public void testIsNotSymlink() throws IOException{
    Type type = new Type();
    addArgument(type, "l");
    type.initialise(new FindOptions());
    
    when(mockFstat.isSymlink()).thenReturn(false);
    PathData item = new PathData("/one/two/test", conf);

    assertEquals(Result.FAIL, type.apply(item));
  }

  @Test
  public void testIsFile() throws IOException{
    Type type = new Type();
    addArgument(type, "f");
    type.initialise(new FindOptions());
    
    when(mockFstat.isFile()).thenReturn(true);
    PathData item = new PathData("/one/two/test", conf);

    assertEquals(Result.PASS, type.apply(item));
  }

  @Test
  public void testIsNotFile() throws IOException{
    Type type = new Type();
    addArgument(type, "f");
    type.initialise(new FindOptions());
    
    when(mockFstat.isFile()).thenReturn(false);
    PathData item = new PathData("/one/two/test", conf);

    assertEquals(Result.FAIL, type.apply(item));
  }

  @Test
  public void testInvalidType() throws IOException {
    Type type = new Type();
    addArgument(type, "a");
    try {
      type.initialise(new FindOptions());
      fail("Invalid file type not caught");
    }
    catch (IOException e) {}
  }
}
