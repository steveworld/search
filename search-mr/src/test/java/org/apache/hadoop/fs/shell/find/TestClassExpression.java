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
package org.apache.hadoop.fs.shell.find;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.fs.shell.find.ClassExpression;
import org.apache.hadoop.fs.shell.find.Expression;
import org.apache.hadoop.fs.shell.find.FilterExpression;
import org.apache.hadoop.fs.shell.find.FindOptions;
import org.apache.hadoop.fs.shell.find.Result;

import org.junit.Before;
import org.junit.Test;

public class TestClassExpression extends TestExpression {
  private Expression expr;
  private FilterExpression test;
  
  @Before
  public void setup() {
    test = new ClassExpression();
    expr = mock(Expression.class);
    TestExpression.baseExpression = expr;
  }
  
  @Test
  public void addArguments() throws IOException {
    test.addArguments(getArgs(TestExpression.class.getName() + " arg1 arg2 arg3"));
    verify(expr).addArguments(getArgs("arg1 arg2 arg3"));
  }

  @Test
  public void addChildren() throws IOException {
    @SuppressWarnings("unchecked") LinkedList<Expression> children = mock(LinkedList.class);
    test.addArguments(getArgs(TestExpression.class.getName()));
    test.addChildren(children);
    verify(expr).addArguments(new LinkedList<String>());
    verify(expr).addChildren(children);
    verifyNoMoreInteractions(expr);
    verifyNoMoreInteractions(children);
  }

  @Test
  public void initialise() throws IOException {
    FindOptions options = new FindOptions();
    test.addArguments(getArgs(TestExpression.class.getName()));
    test.initialise(options);
    verify(expr).addArguments(new LinkedList<String>());
    verify(expr).initialise(options);
    verifyNoMoreInteractions(expr);
  }
  
  @Test
  public void applyPass() throws IOException {
    PathData item = mock(PathData.class);
    when(expr.apply(item)).thenReturn(Result.PASS);
    test.addArguments(getArgs(TestExpression.class.getName()));
    assertEquals(Result.PASS, test.apply(item));
    verify(expr).addArguments(new LinkedList<String>());
    verify(expr).apply(item);
    verifyNoMoreInteractions(expr);
  }
  
  @Test
  public void applyFail() throws IOException {
    PathData item = mock(PathData.class);
    when(expr.apply(item)).thenReturn(Result.FAIL);
    test.addArguments(getArgs(TestExpression.class.getName()));
    assertEquals(Result.FAIL, test.apply(item));
    verify(expr).addArguments(new LinkedList<String>());
    verify(expr).apply(item);
    verifyNoMoreInteractions(expr);
  }
  
  @Test
  public void applyStop() throws IOException {
    PathData item = mock(PathData.class);
    when(expr.apply(item)).thenReturn(Result.STOP);
    test.addArguments(getArgs(TestExpression.class.getName()));
    assertEquals(Result.STOP, test.apply(item));
    verify(expr).addArguments(new LinkedList<String>());
    verify(expr).apply(item);
    verifyNoMoreInteractions(expr);
  }
  
  @Test
  public void finish() throws IOException {
    test.addArguments(getArgs(TestExpression.class.getName()));
    test.finish();
    verify(expr).addArguments(new LinkedList<String>());
    verify(expr).finish();
    verifyNoMoreInteractions(expr);
  }
  
  @Test
  public void isOperator() throws IOException {
    when(expr.isOperator()).thenReturn(true).thenReturn(false);
    test.addArguments(getArgs(TestExpression.class.getName()));
    assertTrue(test.isOperator());
    assertFalse(test.isOperator());
    verify(expr).addArguments(new LinkedList<String>());
    verify(expr, times(2)).isOperator();
    verifyNoMoreInteractions(expr);
  }
  
  @Test
  public void isAction() throws IOException {
    when(expr.isAction()).thenReturn(true).thenReturn(false);
    test.addArguments(getArgs(TestExpression.class.getName()));
    assertTrue(test.isAction());
    assertFalse(test.isAction());
    verify(expr).addArguments(new LinkedList<String>());
    verify(expr, times(2)).isAction();
    verifyNoMoreInteractions(expr);
  }
  
  @Test
  public void getPrecedence() throws IOException {
    when(expr.getPrecedence()).thenReturn(12345).thenReturn(67890);
    test.addArguments(getArgs(TestExpression.class.getName()));
    assertEquals(12345, test.getPrecedence());
    assertEquals(67890, test.getPrecedence());
    verify(expr).addArguments(new LinkedList<String>());
    verify(expr, times(2)).getPrecedence();
    verifyNoMoreInteractions(expr);
  }
  
  public static class TestExpression extends FilterExpression {
    static Expression baseExpression = null;
    public TestExpression() {
      super(baseExpression);
    }
  }
}
