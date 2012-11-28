package org.apache.hadoop.fs.shell.find;

import static org.junit.Assert.*;

import org.apache.hadoop.fs.shell.find.Result;
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
