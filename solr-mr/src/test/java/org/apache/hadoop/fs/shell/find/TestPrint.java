package org.apache.hadoop.fs.shell.find;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.IOException;

import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.fs.shell.find.FindOptions;
import org.apache.hadoop.fs.shell.find.Print;
import org.apache.hadoop.fs.shell.find.Result;
import org.junit.Test;

import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Before;

public class TestPrint extends TestExpression {
  private static FileSystem fs;
  private static Configuration conf;

  @Before
  public void resetMock() throws IOException {
    MockFileSystem.reset();
    fs = new MockFileSystem();
    conf = fs.getConf();
  }
  
  @Test
  public void testPrint() throws IOException{
    Print print = new Print();
    PrintStream out = mock(PrintStream.class);
    FindOptions options = new FindOptions();
    options.setOut(out);
    print.initialise(options);
    
    String filename = "/one/two/test";
    PathData item = new PathData(filename, conf);
    assertEquals(Result.PASS, print.apply(item));
    verify(out).println(filename);
    verifyNoMoreInteractions(out);
  }
}
