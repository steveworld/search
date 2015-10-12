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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.IOException;
import java.util.Deque;
import java.util.LinkedList;

import org.apache.hadoop.fs.shell.PathData;
import org.apache.solr.hadoop.fs.shell.find.And;
import org.apache.solr.hadoop.fs.shell.find.Expression;
import org.apache.solr.hadoop.fs.shell.find.FindOptions;
import org.apache.solr.hadoop.fs.shell.find.Result;
import org.junit.Test;

public class TestAnd extends TestExpression {

  @Test
  public void testPass()  throws IOException {
    And and = new And();
    
    PathData pathData = mock(PathData.class);
    
    Expression first = mock(Expression.class);
    when(first.apply(pathData)).thenReturn(Result.PASS);
    
    Expression second = mock(Expression.class);
    when(second.apply(pathData)).thenReturn(Result.PASS);
    
    Deque<Expression> children = new LinkedList<Expression>();
    children.add(second);
    children.add(first);
    and.addChildren(children);

    assertEquals(Result.PASS, and.apply(pathData));
    verify(first).apply(pathData);
    verify(second).apply(pathData);
    verifyNoMoreInteractions(first);
    verifyNoMoreInteractions(second);
  }

  @Test
  public void testFailFirst()  throws IOException {
    And and = new And();
    
    PathData pathData = mock(PathData.class);
    
    Expression first = mock(Expression.class);
    when(first.apply(pathData)).thenReturn(Result.FAIL);
    
    Expression second = mock(Expression.class);
    when(second.apply(pathData)).thenReturn(Result.PASS);
    
    Deque<Expression> children = new LinkedList<Expression>();
    children.add(second);
    children.add(first);
    and.addChildren(children);
    
    assertEquals(Result.FAIL, and.apply(pathData));
    verify(first).apply(pathData);
    verifyNoMoreInteractions(first);
    verifyNoMoreInteractions(second);
  }

  @Test
  public void testFailSecond()  throws IOException {
    And and = new And();
    
    PathData pathData = mock(PathData.class);
    
    Expression first = mock(Expression.class);
    when(first.apply(pathData)).thenReturn(Result.PASS);
    
    Expression second = mock(Expression.class);
    when(second.apply(pathData)).thenReturn(Result.FAIL);
    
    Deque<Expression> children = new LinkedList<Expression>();
    children.add(second);
    children.add(first);
    and.addChildren(children);
    
    assertEquals(Result.FAIL, and.apply(pathData));
    verify(first).apply(pathData);
    verify(second).apply(pathData);
    verifyNoMoreInteractions(first);
    verifyNoMoreInteractions(second);
  }

  @Test
  public void testFailBoth()  throws IOException {
    And and = new And();
    
    PathData pathData = mock(PathData.class);
    
    Expression first = mock(Expression.class);
    when(first.apply(pathData)).thenReturn(Result.FAIL);
    
    Expression second = mock(Expression.class);
    when(second.apply(pathData)).thenReturn(Result.FAIL);
    
    Deque<Expression> children = new LinkedList<Expression>();
    children.add(second);
    children.add(first);
    and.addChildren(children);
    
    assertEquals(Result.FAIL, and.apply(pathData));
    verify(first).apply(pathData);
    verifyNoMoreInteractions(first);
    verifyNoMoreInteractions(second);
  }

  @Test
  public void testStopFirst()  throws IOException {
    And and = new And();
    
    PathData pathData = mock(PathData.class);
    
    Expression first = mock(Expression.class);
    when(first.apply(pathData)).thenReturn(Result.STOP);
    
    Expression second = mock(Expression.class);
    when(second.apply(pathData)).thenReturn(Result.PASS);
    
    Deque<Expression> children = new LinkedList<Expression>();
    children.add(second);
    children.add(first);
    and.addChildren(children);
    
    assertEquals(Result.STOP, and.apply(pathData));
    verify(first).apply(pathData);
    verify(second).apply(pathData);
    verifyNoMoreInteractions(first);
    verifyNoMoreInteractions(second);
  }

  @Test
  public void testStopSecond()  throws IOException {
    And and = new And();
    
    PathData pathData = mock(PathData.class);
    
    Expression first = mock(Expression.class);
    when(first.apply(pathData)).thenReturn(Result.PASS);
    
    Expression second = mock(Expression.class);
    when(second.apply(pathData)).thenReturn(Result.STOP);
    
    Deque<Expression> children = new LinkedList<Expression>();
    children.add(second);
    children.add(first);
    and.addChildren(children);
    
    assertEquals(Result.STOP, and.apply(pathData));
    verify(first).apply(pathData);
    verify(second).apply(pathData);
    verifyNoMoreInteractions(first);
    verifyNoMoreInteractions(second);
  }

  @Test
  public void testStopFail()  throws IOException {
    And and = new And();
    
    PathData pathData = mock(PathData.class);
    
    Expression first = mock(Expression.class);
    when(first.apply(pathData)).thenReturn(Result.STOP);
    
    Expression second = mock(Expression.class);
    when(second.apply(pathData)).thenReturn(Result.FAIL);
    
    Deque<Expression> children = new LinkedList<Expression>();
    children.add(second);
    children.add(first);
    and.addChildren(children);
    
    assertEquals(Result.STOP.combine(Result.FAIL), and.apply(pathData));
    verify(first).apply(pathData);
    verify(second).apply(pathData);
    verifyNoMoreInteractions(first);
    verifyNoMoreInteractions(second);
  }
  
  @Test
  public void testInit() throws IOException {
    And and = new And();
    Expression first = mock(Expression.class);
    Expression second = mock(Expression.class);

    Deque<Expression> children = new LinkedList<Expression>();
    children.add(second);
    children.add(first);
    and.addChildren(children);
    
    FindOptions options = mock(FindOptions.class);
    and.initialise(options);
    verify(first).initialise(options);
    verify(second).initialise(options);
    verifyNoMoreInteractions(first);
    verifyNoMoreInteractions(second);
  }
}
