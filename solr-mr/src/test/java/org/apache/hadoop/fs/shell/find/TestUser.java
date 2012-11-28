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
import org.apache.hadoop.fs.shell.find.User;
import org.junit.Before;
import org.junit.Test;

public class TestUser extends TestExpression {
  private MockFileSystem fs;
  private Configuration conf;
  private FileStatus fileStatus;
  private PathData item;
  private User user;

  @Before
  public void setUp() throws IOException {
    MockFileSystem.reset();
    fs = new MockFileSystem();
    conf = fs.getConf();
    fileStatus = mock(FileStatus.class);
    fs.setFileStatus("test", fileStatus);
    item = new PathData("/one/two/test", conf);

    user = new User();
    addArgument(user, "user");
    user.initialise(new FindOptions());
  }
  
  @Test
  public void applyPass() throws IOException{
    when(fileStatus.getOwner()).thenReturn("user");
    
    assertEquals(Result.PASS, user.apply(item));
  }
  
  @Test
  public void applyFail() throws IOException {
    when(fileStatus.getOwner()).thenReturn("notuser");
    
    assertEquals(Result.FAIL, user.apply(item));
  }

  @Test
  public void applyBlank() throws IOException {
    when(fileStatus.getOwner()).thenReturn("");
    
    assertEquals(Result.FAIL, user.apply(item));
  }

  @Test
  public void applyNull() throws IOException {
    when(fileStatus.getOwner()).thenReturn(null);
    
    assertEquals(Result.FAIL, user.apply(item));
  }
}
