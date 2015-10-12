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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.IOException;
import java.util.Deque;

import org.apache.hadoop.fs.shell.PathData;
import org.apache.solr.hadoop.fs.shell.find.Expression;
import org.apache.solr.hadoop.fs.shell.find.FilterExpression;
import org.apache.solr.hadoop.fs.shell.find.FindOptions;
import org.apache.solr.hadoop.fs.shell.find.Result;

import org.junit.Before;
import org.junit.Test;

public class TestFilterExpression extends TestExpression {
  private Expression expr;
  private FilterExpression test;
  
  @Before
  public void setup() {
    expr = mock(Expression.class);
    test = new FilterExpression(expr){};
  }
  
  @Test
  public void expression() throws IOException {
    assertEquals(expr, test.expression);
  }

  @Test
  public void initialise() throws IOException {
    FindOptions options = mock(FindOptions.class);
    test.initialise(options);
    verify(expr).initialise(options);
    verifyNoMoreInteractions(expr);
  }
  
  @Test
  public void apply() throws IOException {
    PathData item = mock(PathData.class);
    when(expr.apply(item)).thenReturn(Result.PASS).thenReturn(Result.FAIL);
    assertEquals(Result.PASS, test.apply(item));
    assertEquals(Result.FAIL, test.apply(item));
    verify(expr, times(2)).apply(item);
    verifyNoMoreInteractions(expr);
  }
  
  @Test
  public void finish() throws IOException {
    test.finish();
    verify(expr).finish();
    verifyNoMoreInteractions(expr);
  }

  @Test
  public void getUsage() {
    String[] usage = new String[]{"Usage 1", "Usage 2", "Usage 3"};
    when(expr.getUsage()).thenReturn(usage);
    assertArrayEquals(usage, test.getUsage());
    verify(expr).getUsage();
    verifyNoMoreInteractions(expr);
  }

  @Test
  public void getHelp() {
    String[] help = new String[]{"Help 1", "Help 2", "Help 3"};
    when(expr.getHelp()).thenReturn(help);
    assertArrayEquals(help, test.getHelp());
    verify(expr).getHelp();
    verifyNoMoreInteractions(expr);
  }
  
  @Test
  public void isAction() {
    when(expr.isAction()).thenReturn(true).thenReturn(false);
    assertTrue(test.isAction());
    assertFalse(test.isAction());
    verify(expr, times(2)).isAction();
    verifyNoMoreInteractions(expr);
  }

  @Test
  public void isOperator() {
    when(expr.isAction()).thenReturn(true).thenReturn(false);
    assertTrue(test.isAction());
    assertFalse(test.isAction());
    verify(expr, times(2)).isAction();
    verifyNoMoreInteractions(expr);
  }
  
  @Test
  public void getPrecedence() {
    int precedence = 12345;
    when(expr.getPrecedence()).thenReturn(precedence);
    assertEquals(precedence, test.getPrecedence());
    verify(expr).getPrecedence();
    verifyNoMoreInteractions(expr);
  }
  
  @Test
  public void addChildren() {
    @SuppressWarnings("unchecked") Deque<Expression> expressions = mock(Deque.class);
    test.addChildren(expressions);
    verify(expr).addChildren(expressions);
    verifyNoMoreInteractions(expr);
  }
  
  @Test
  public void addArguments() {
    @SuppressWarnings("unchecked") Deque<String> args = mock(Deque.class);
    test.addArguments(args);
    verify(expr).addArguments(args);
    verifyNoMoreInteractions(expr);
  }
}
