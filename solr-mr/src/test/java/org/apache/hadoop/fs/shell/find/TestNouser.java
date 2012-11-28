package org.apache.hadoop.fs.shell.find;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.fs.shell.find.FindOptions;
import org.apache.hadoop.fs.shell.find.Nouser;
import org.apache.hadoop.fs.shell.find.Result;
import org.junit.Before;
import org.junit.Test;

public class TestNouser extends TestExpression {
  private MockFileSystem fs;
  private Configuration conf;
  private FileStatus fileStatus;
  private PathData item;
  private Nouser nouser;

  @Before
  public void setUp() throws IOException {
    MockFileSystem.reset();
    fs = new MockFileSystem();
    conf = fs.getConf();
    fileStatus = mock(FileStatus.class);
    fs.setFileStatus("test", fileStatus);
    item = new PathData("/one/two/test", conf);

    nouser = new Nouser();
    nouser.initialise(new FindOptions());
  }
  
  @Test
  public void applyPass() throws IOException{
    when(fileStatus.getOwner()).thenReturn("user");
    
    assertEquals(Result.FAIL, nouser.apply(item));
  }
  
  @Test
  public void applyFail() throws IOException {
    when(fileStatus.getOwner()).thenReturn("notuser");
    
    assertEquals(Result.FAIL, nouser.apply(item));
  }

  @Test
  public void applyBlank() throws IOException {
    when(fileStatus.getOwner()).thenReturn("");
    
    assertEquals(Result.PASS, nouser.apply(item));
  }

  @Test
  public void applyNull() throws IOException {
    when(fileStatus.getOwner()).thenReturn(null);
    
    assertEquals(Result.PASS, nouser.apply(item));
  }
}
