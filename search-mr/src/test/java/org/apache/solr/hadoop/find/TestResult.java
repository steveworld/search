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

import org.apache.solr.hadoop.fs.shell.find.Result;
import org.junit.Test;

public class TestResult extends TestExpression {

  @Test
  public void testPass() {
    Result result = Result.PASS;
    assertTrue(result.isPass());
    assertTrue(result.isDescend());
  }

  @Test
  public void testFail() {
    Result result = Result.FAIL;
    assertFalse(result.isPass());
    assertTrue(result.isDescend());
  }
  @Test
  public void testStop() {
    Result result = Result.STOP;
    assertTrue(result.isPass());
    assertFalse(result.isDescend());
  }

  @Test
  public void combinePassPass() {
    Result result = Result.PASS.combine(Result.PASS);
    assertTrue(result.isPass());
    assertTrue(result.isDescend());
  }

  @Test
  public void combinePassFail() {
    Result result = Result.PASS.combine(Result.FAIL);
    assertFalse(result.isPass());
    assertTrue(result.isDescend());
  }
  @Test
  public void combineFailPass() {
    Result result = Result.FAIL.combine(Result.PASS);
    assertFalse(result.isPass());
    assertTrue(result.isDescend());
  }
  @Test
  public void combineFailFail() {
    Result result = Result.FAIL.combine(Result.FAIL);
    assertFalse(result.isPass());
    assertTrue(result.isDescend());
  }
  @Test
  public void combinePassStop() {
    Result result = Result.PASS.combine(Result.STOP);
    assertTrue(result.isPass());
    assertFalse(result.isDescend());
  }

  @Test
  public void combineStopFail() {
    Result result = Result.STOP.combine(Result.FAIL);
    assertFalse(result.isPass());
    assertFalse(result.isDescend());
  }
  @Test
  public void combineStopPass() {
    Result result = Result.STOP.combine(Result.PASS);
    assertTrue(result.isPass());
    assertFalse(result.isDescend());
  }
  @Test
  public void combineFailStop() {
    Result result = Result.FAIL.combine(Result.STOP);
    assertFalse(result.isPass());
    assertFalse(result.isDescend());
  }
  
  @Test
  public void negatePass() {
    Result result = Result.PASS.negate();
    assertFalse(result.isPass());
    assertTrue(result.isDescend());
  }

  @Test
  public void negateFail() {
    Result result = Result.FAIL.negate();
    assertTrue(result.isPass());
    assertTrue(result.isDescend());
  }
  @Test
  public void negateStop() {
    Result result = Result.STOP.negate();
    assertFalse(result.isPass());
    assertFalse(result.isDescend());
  }
  @Test
  public void equalsPass() {
    Result one = Result.PASS;
    Result two = Result.PASS.combine(Result.PASS);
    assertEquals(one, two);
  }
  @Test
  public void equalsFail() {
    Result one = Result.FAIL;
    Result two = Result.FAIL.combine(Result.FAIL);
    assertEquals(one, two);
  }
  @Test
  public void equalsStop() {
    Result one = Result.STOP;
    Result two = Result.STOP.combine(Result.STOP);
    assertEquals(one, two);
  }
  @Test
  public void notEquals() {
    assertFalse(Result.PASS.equals(Result.FAIL));
    assertFalse(Result.PASS.equals(Result.STOP));
    assertFalse(Result.FAIL.equals(Result.PASS));
    assertFalse(Result.FAIL.equals(Result.STOP));
    assertFalse(Result.STOP.equals(Result.PASS));
    assertFalse(Result.STOP.equals(Result.FAIL));
  }
}
