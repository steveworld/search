package org.apache.hadoop.fs.shell.find;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.fs.shell.find.Depth;
import org.apache.hadoop.fs.shell.find.FindOptions;
import org.apache.hadoop.fs.shell.find.Result;
import org.junit.Test;

public class TestDepth extends TestExpression {
  @Test
  public void initialise() throws IOException{
    FindOptions options = new FindOptions();
    Depth depth = new Depth();
    
    assertFalse(options.isDepth());
    depth.initialise(options);
    assertTrue(options.isDepth());
  }

  @Test
  public void apply() throws IOException{
    Depth depth = new Depth();
    depth.initialise(new FindOptions());
    assertEquals(Result.PASS, depth.apply(new PathData("anything", new Configuration())));
  }
}
