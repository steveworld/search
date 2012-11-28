package org.apache.hadoop.fs.shell.find;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.fs.shell.find.FindOptions;
import org.apache.hadoop.fs.shell.find.Name;
import org.apache.hadoop.fs.shell.find.Result;
import org.junit.Before;
import org.junit.Test;

public class TestIname extends TestExpression {
  private static FileSystem fs;
  private static Configuration conf;
  private Name.Iname name;

  @Before
  public void setUp() throws IOException {
    MockFileSystem.reset();
    fs = new MockFileSystem();
    conf = fs.getConf();

    name = new Name.Iname();
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
    assertEquals(Result.PASS, name.apply(item));
  }
}
