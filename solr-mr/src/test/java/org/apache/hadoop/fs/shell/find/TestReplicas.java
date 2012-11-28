package org.apache.hadoop.fs.shell.find;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.fs.shell.find.FindOptions;
import org.apache.hadoop.fs.shell.find.Replicas;
import org.apache.hadoop.fs.shell.find.Result;
import org.junit.Test;
import org.junit.Before;

public class TestReplicas extends TestExpression {
  private MockFileSystem fs;
  private PathData one;
  private PathData two;
  private PathData three;
  private PathData four;
  private PathData five;
  
  @Before
  public void setUp() throws IOException {
    MockFileSystem.reset();
    fs = new MockFileSystem();
    
    FileStatus fileStatus;
    
    fileStatus = mock(FileStatus.class);
    when(fileStatus.getReplication()).thenReturn((short) 1);
    fs.setFileStatus("one", fileStatus);
    one = new PathData("one", fs.getConf());

    fileStatus = mock(FileStatus.class);
    when(fileStatus.getReplication()).thenReturn((short)2);
    fs.setFileStatus("two", fileStatus);
    two = new PathData("two", fs.getConf());

    fileStatus = mock(FileStatus.class);
    when(fileStatus.getReplication()).thenReturn((short)3);
    fs.setFileStatus("three", fileStatus);
    three = new PathData("three", fs.getConf());

    fileStatus = mock(FileStatus.class);
    when(fileStatus.getReplication()).thenReturn((short)4);
    fs.setFileStatus("four", fileStatus);
    four = new PathData("four", fs.getConf());

    fileStatus = mock(FileStatus.class);
    when(fileStatus.getReplication()).thenReturn((short)5);
    fs.setFileStatus("five", fileStatus);
    five = new PathData("five", fs.getConf());
  }

  @Test
  public void applyEquals() throws IOException {
    Replicas rep = new Replicas();
    addArgument(rep, "3");
    rep.initialise(new FindOptions());

    assertEquals(Result.FAIL, rep.apply(one));
    assertEquals(Result.FAIL, rep.apply(two));
    assertEquals(Result.PASS, rep.apply(three));
    assertEquals(Result.FAIL, rep.apply(four));
    assertEquals(Result.FAIL, rep.apply(five));
  }

  @Test
  public void applyGreaterThan() throws IOException {
    Replicas rep = new Replicas();
    addArgument(rep, "+3");
    rep.initialise(new FindOptions());

    assertEquals(Result.FAIL, rep.apply(one));
    assertEquals(Result.FAIL, rep.apply(two));
    assertEquals(Result.FAIL, rep.apply(three));
    assertEquals(Result.PASS, rep.apply(four));
    assertEquals(Result.PASS, rep.apply(five));
  }

  @Test
  public void applyLessThan() throws IOException {
    Replicas rep = new Replicas();
    addArgument(rep, "-3");
    rep.initialise(new FindOptions());

    assertEquals(Result.PASS, rep.apply(one));
    assertEquals(Result.PASS, rep.apply(two));
    assertEquals(Result.FAIL, rep.apply(three));
    assertEquals(Result.FAIL, rep.apply(four));
    assertEquals(Result.FAIL, rep.apply(five));
  }
}
