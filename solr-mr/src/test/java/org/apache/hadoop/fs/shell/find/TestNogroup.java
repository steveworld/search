package org.apache.hadoop.fs.shell.find;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.fs.shell.find.FindOptions;
import org.apache.hadoop.fs.shell.find.Nogroup;
import org.apache.hadoop.fs.shell.find.Result;
import org.junit.Before;
import org.junit.Test;

public class TestNogroup extends TestExpression {
  private MockFileSystem fs;
  private Configuration conf;
  private FileStatus fileStatus;
  private PathData item;
  private Nogroup nogroup;

  @Before
  public void setUp() throws IOException {
    MockFileSystem.reset();
    fs = new MockFileSystem();
    conf = fs.getConf();
    fileStatus = mock(FileStatus.class);
    fs.setFileStatus("test", fileStatus);
    item = new PathData("/one/two/test", conf);

    nogroup = new Nogroup();
    nogroup.initialise(new FindOptions());
  }
  
  @Test
  public void applyPass() throws IOException{
    when(fileStatus.getGroup()).thenReturn("user");
    
    assertEquals(Result.FAIL, nogroup.apply(item));
  }
  
  @Test
  public void applyFail() throws IOException {
    when(fileStatus.getGroup()).thenReturn("notuser");
    
    assertEquals(Result.FAIL, nogroup.apply(item));
  }

  @Test
  public void applyBlank() throws IOException {
    when(fileStatus.getGroup()).thenReturn("");
    
    assertEquals(Result.PASS, nogroup.apply(item));
  }

  @Test
  public void applyNull() throws IOException {
    when(fileStatus.getGroup()).thenReturn(null);
    
    assertEquals(Result.PASS, nogroup.apply(item));
  }
}
