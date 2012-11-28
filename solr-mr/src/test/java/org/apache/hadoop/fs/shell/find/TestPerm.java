package org.apache.hadoop.fs.shell.find;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.fs.shell.find.FindOptions;
import org.apache.hadoop.fs.shell.find.Perm;
import org.apache.hadoop.fs.shell.find.Result;
import org.junit.Test;
import org.junit.Before;

public class TestPerm extends TestExpression {
  private MockFileSystem fs;
  private PathData rwxrwxrwx;
  private PathData rwx______;
  private PathData r__r__r__;
  private PathData rwxr_____;
  private PathData __x_w__wx;
  
  @Before
  public void setUp() throws IOException {
    MockFileSystem.reset();
    fs = new MockFileSystem();
    
    FileStatus fileStatus;
    
    fileStatus = mock(FileStatus.class);
    when(fileStatus.getPermission()).thenReturn(FsPermission.valueOf("-rwxrwxrwx"));
    fs.setFileStatus("rwxrwxrwx", fileStatus);
    rwxrwxrwx = new PathData("rwxrwxrwx", fs.getConf());

    fileStatus = mock(FileStatus.class);
    when(fileStatus.getPermission()).thenReturn(FsPermission.valueOf("-rwx------"));
    fs.setFileStatus("rwx______", fileStatus);
    rwx______ = new PathData("rwx______", fs.getConf());

    fileStatus = mock(FileStatus.class);
    when(fileStatus.getPermission()).thenReturn(FsPermission.valueOf("-r--r--r--"));
    fs.setFileStatus("r__r__r__", fileStatus);
    r__r__r__ = new PathData("r__r__r__", fs.getConf());

    fileStatus = mock(FileStatus.class);
    when(fileStatus.getPermission()).thenReturn(FsPermission.valueOf("-rwxr-----"));
    fs.setFileStatus("rwxr_____", fileStatus);
    rwxr_____ = new PathData("rwxr_____", fs.getConf());

    fileStatus = mock(FileStatus.class);
    when(fileStatus.getPermission()).thenReturn(FsPermission.valueOf("---x-w--wx"));
    fs.setFileStatus("__x_w__wx", fileStatus);
    __x_w__wx = new PathData("__x_w__wx", fs.getConf());
  }

  @Test
  public void applyOctalExact() throws IOException {
    Perm perm = new Perm();
    addArgument(perm, "123");
    perm.initialise(new FindOptions());

    assertEquals(Result.FAIL, perm.apply(rwxrwxrwx));
    assertEquals(Result.FAIL, perm.apply(rwx______));
    assertEquals(Result.FAIL, perm.apply(r__r__r__));
    assertEquals(Result.FAIL, perm.apply(rwxr_____));
    assertEquals(Result.PASS, perm.apply(__x_w__wx));
  }

  @Test
  public void applyOctalMask() throws IOException {
    Perm perm = new Perm();
    addArgument(perm, "-123");
    perm.initialise(new FindOptions());

    assertEquals(Result.PASS, perm.apply(rwxrwxrwx));
    assertEquals(Result.FAIL, perm.apply(rwx______));
    assertEquals(Result.FAIL, perm.apply(r__r__r__));
    assertEquals(Result.FAIL, perm.apply(rwxr_____));
    assertEquals(Result.PASS, perm.apply(__x_w__wx));
  }

  @Test
  public void applySymbolicExact() throws IOException {
    Perm perm = new Perm();
    addArgument(perm, "u=x,g=w,o=wx");
    perm.initialise(new FindOptions());

    assertEquals(Result.FAIL, perm.apply(rwxrwxrwx));
    assertEquals(Result.FAIL, perm.apply(rwx______));
    assertEquals(Result.FAIL, perm.apply(r__r__r__));
    assertEquals(Result.FAIL, perm.apply(rwxr_____));
    assertEquals(Result.PASS, perm.apply(__x_w__wx));
  }

  @Test
  public void applySymbolicMask() throws IOException {
    Perm perm = new Perm();
    addArgument(perm, "-u=x,g=w,o=wx");
    perm.initialise(new FindOptions());

    assertEquals(Result.PASS, perm.apply(rwxrwxrwx));
    assertEquals(Result.FAIL, perm.apply(rwx______));
    assertEquals(Result.FAIL, perm.apply(r__r__r__));
    assertEquals(Result.FAIL, perm.apply(rwxr_____));
    assertEquals(Result.PASS, perm.apply(__x_w__wx));
  }

  @Test
  public void applySymbolicComplex() throws IOException {
    Perm perm = new Perm();
    addArgument(perm, "u=xrw,g=x,o=wx,u-rw,g+w,g-x");
    perm.initialise(new FindOptions());

    assertEquals(Result.FAIL, perm.apply(rwxrwxrwx));
    assertEquals(Result.FAIL, perm.apply(rwx______));
    assertEquals(Result.FAIL, perm.apply(r__r__r__));
    assertEquals(Result.FAIL, perm.apply(rwxr_____));
    assertEquals(Result.PASS, perm.apply(__x_w__wx));
  }
}
