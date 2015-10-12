/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.hadoop.fs.shell.find;

import static org.junit.Assert.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.IOException;
import java.io.PrintStream;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.Command;
import org.apache.hadoop.fs.shell.CommandFactory;
import org.apache.hadoop.fs.shell.FsCommand;
import org.apache.hadoop.fs.shell.PathData;
import org.apache.solr.hadoop.fs.shell.find.Exec;
import org.apache.solr.hadoop.fs.shell.find.FindOptions;
import org.apache.solr.hadoop.fs.shell.find.Result;

import org.junit.Before;
import org.junit.Test;

public class TestExec extends TestExpression {
  private MockFileSystem fs;
  private Configuration conf;
  private FindOptions options;
  private CommandFactory factory;
  private PathData item;
  private PrintStream out;
  private PrintStream err;

  @Before
  public void resetMock() throws IOException {
    MockFileSystem.reset();
    fs = new MockFileSystem();
    conf = fs.getConf();
    out = mock(PrintStream.class);
    err = mock(PrintStream.class);

    factory = new CommandFactory(conf);
    factory.registerCommands(TestCommand.class);
    options = new FindOptions();
    options.setCommandFactory(factory);
    options.setOut(out);
    
    String pathname = "/one/two/test";
    
    FileStatus fileStatus = mock(FileStatus.class);
    when(fileStatus.getPath()).thenReturn(new Path(pathname));
    
    fs.setFileStatus("test", fileStatus);
    fs.setGlobStatus("test", new FileStatus[]{fileStatus});
    item = new PathData(pathname, fs.getConf());
    
    TestCommand.testOut = out;
    TestCommand.testErr = err;
    TestCommand.testConf = conf;
  }
  
  @Test
  public void addArguments() throws IOException {
    Exec exec = new Exec();
    exec.addArguments(getArgs("one two three ; four"));
    assertEquals("Exec(one,two,three;)", exec.toString());
    assertFalse(exec.isBatch());
  }

  @Test
  public void addArgumentsBracket() throws IOException {
    Exec exec = new Exec();
    exec.addArguments(getArgs("one {} three ; four"));
    assertEquals("Exec(one,{},three;)", exec.toString());
    assertFalse(exec.isBatch());
  }

  @Test
  public void addArgumentsPlus() throws IOException {
    Exec exec = new Exec();
    exec.addArguments(getArgs("one two three + four"));
    assertEquals("Exec(one,two,three,+,four;)", exec.toString());
    assertFalse(exec.isBatch());
  }

  @Test
  public void addArgumentsBracketPlus() throws IOException {
    Exec exec = new Exec();
    exec.addArguments(getArgs("one two {} + four"));
    assertEquals("Exec(one,two,{};)", exec.toString());
    assertTrue(exec.isBatch());
  }

  @Test
  public void testFsShellCommand() throws IOException {
    factory.registerCommands(FsCommand.class);
    Exec exec = new Exec();
    exec.addArguments(getArgs("-ls {} ;"));
    exec.initialise(options);
    Command cmd = exec.getCommand();
    assertEquals("org.apache.hadoop.fs.shell.Ls", cmd.getClass().getName());
  }
  
  @Test
  public void testUnknownCommand() throws IOException {
    TestCommand.testErr = System.err;
    Exec exec = new Exec();
    exec.addArguments(getArgs("-invalid arg1 arg2 ;"));
    try {
      exec.initialise(options);
      fail("Invalid command not caught");
    }
    catch(IOException e) {
      assertEquals("Unknown command: -invalid", e.getMessage());
    }
    verifyNoMoreInteractions(out);
    verifyNoMoreInteractions(err);
  }

  @Test
  public void testTestCommand() throws IOException {
    Exec exec = new Exec();
    exec.addArguments(getArgs("-testCommand ;"));
    exec.initialise(options);
    Command cmd = exec.getCommand();
    assertEquals(TestCommand.class, cmd.getClass());
    verifyNoMoreInteractions(out);
    verifyNoMoreInteractions(err);
  }
  
  @Test
  public void applyOneArg() throws IOException {
    Exec exec = new Exec();
    exec.addArguments(getArgs("-testCommand {} ;"));
    exec.initialise(options);
    assertEquals(Result.PASS, exec.apply(item));
    verify(out).println("TestCommand.processPath:"+item.toString());
    verifyNoMoreInteractions(out);
    verifyNoMoreInteractions(err);
  }
  
  @Test
  public void applyOptions() throws IOException {
    Exec exec = new Exec();
    exec.addArguments(getArgs("-testCommand -option1 -option2 {} ;"));
    exec.initialise(options);
    assertEquals(Result.PASS, exec.apply(item));
    verify(out).println("TestCommand.processOptions:-option1");
    verify(out).println("TestCommand.processOptions:-option2");
    verify(out).println("TestCommand.processPath:"+item.toString());
    verifyNoMoreInteractions(out);
    verifyNoMoreInteractions(err);
  }
  
  @Test
  public void applyFail() throws IOException {
    Exec exec = new Exec();
    exec.addArguments(getArgs("-testCommand -fail {} ;"));
    exec.initialise(options);
    assertEquals(Result.FAIL, exec.apply(item));
    verify(out).println("TestCommand.processOptions:-fail");
    verify(out).println("TestCommand.processPath:"+item.toString());
    verifyNoMoreInteractions(out);
    verify(err).println("testCommand: failed");
    verifyNoMoreInteractions(err);
  }
  
  @Test
  public void applyRepeatArg() throws IOException {
    Exec exec = new Exec();
    exec.addArguments(getArgs("-testCommand {} {} ;"));
    exec.initialise(options);
    assertEquals(Result.PASS, exec.apply(item));
    verify(out, times(2)).println("TestCommand.processPath:"+item.toString());
    verifyNoMoreInteractions(out);
    verifyNoMoreInteractions(err);
  }
  
  @Test
  public void applyAdditionalArg() throws IOException {
    Exec exec = new Exec();
    exec.addArguments(getArgs("-testCommand path1 {} path2 ;"));
    exec.initialise(options);
    
    FileStatus fstat1 = mock(FileStatus.class);
    when(fstat1.getPath()).thenReturn(new Path("path1"));
    FileStatus fstat2 = mock(FileStatus.class);
    when(fstat2.getPath()).thenReturn(new Path("path2"));
    fs.setGlobStatus("path1", new FileStatus[]{fstat1});
    fs.setGlobStatus("path2", new FileStatus[]{fstat2});

    assertEquals(Result.PASS, exec.apply(item));
    verify(out).println("TestCommand.processPath:path1");
    verify(out).println("TestCommand.processPath:"+item.toString());
    verify(out).println("TestCommand.processPath:path2");
    verifyNoMoreInteractions(out);
    verifyNoMoreInteractions(err);
  }
  
  @Test
  public void applyBatched() throws IOException {
    Exec exec = new Exec();
    exec.addArguments(getArgs("-testCommand {} +"));
    exec.initialise(options);
    exec.setMaxArgs(2);
    
    FileStatus fstat1 = mock(FileStatus.class);
    when(fstat1.getPath()).thenReturn(new Path("test1"));
    when(fstat1.toString()).thenReturn("test1");
    fs.setFileStatus("test1", fstat1);
    fs.setGlobStatus("test1", new FileStatus[]{fstat1});
    PathData item1 = new PathData("test1", fs.getConf());

    FileStatus fstat2 = mock(FileStatus.class);
    when(fstat2.getPath()).thenReturn(new Path("test2"));
    when(fstat2.toString()).thenReturn("test2");
    fs.setFileStatus("test2", fstat2);
    fs.setGlobStatus("test2", new FileStatus[]{fstat2});
    PathData item2 = new PathData("test2", fs.getConf());

    FileStatus fstat3 = mock(FileStatus.class);
    when(fstat3.getPath()).thenReturn(new Path("test3"));
    when(fstat3.toString()).thenReturn("test3");
    fs.setFileStatus("test3", fstat3);
    fs.setGlobStatus("test3", new FileStatus[]{fstat3});
    PathData item3 = new PathData("test3", fs.getConf());
    
    assertEquals(Result.PASS, exec.apply(item1));
    verifyNoMoreInteractions(out);
    verifyNoMoreInteractions(err);
    
    assertEquals(Result.PASS, exec.apply(item2));
    verify(out).println("TestCommand.processPath:"+item1.toString());
    verify(out).println("TestCommand.processPath:"+item2.toString());
    verifyNoMoreInteractions(out);
    verifyNoMoreInteractions(err);
    
    assertEquals(Result.PASS, exec.apply(item3));
    verifyNoMoreInteractions(out);
    verifyNoMoreInteractions(err);

    exec.finish();
    verify(out).println("TestCommand.processPath:"+item3.toString());
    verifyNoMoreInteractions(out);
    verifyNoMoreInteractions(err);
  }
  
  public static class TestCommand extends Command {
    static PrintStream testOut;
    static PrintStream testErr;
    static Configuration testConf;
    protected TestCommand() {
      this.out = testOut;
      this.err = testErr;
      setConf(testConf);
    }

    private boolean fail = false;
    public static void registerCommands(CommandFactory factory) {
      factory.addClass(TestCommand.class, "-testCommand");
    }
    @Override
    public String getCommandName() { 
      return getName(); 
    }
    
    @Override
    protected void run(Path path) throws IOException {
      throw new RuntimeException("not supposed to get here");
    }
    protected void processOptions(LinkedList<String> args) {
      while(!args.isEmpty()) {
        String arg = args.get(0);
        if(!arg.startsWith("-")) {
          break;
        }
        if("-fail".equals(arg)) {
          fail = true;
        }
        out.println("TestCommand.processOptions:" + arg);
        args.pop();
      }
    }
    protected void processPath(PathData item) throws IOException {
      out.println("TestCommand.processPath:"+item.toString());
      if(fail) {
        throw new IOException("failed");
      }
    }
  }
}
