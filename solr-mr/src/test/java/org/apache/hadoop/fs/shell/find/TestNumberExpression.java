package org.apache.hadoop.fs.shell.find;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.fs.shell.find.FindOptions;
import org.apache.hadoop.fs.shell.find.NumberExpression;
import org.apache.hadoop.fs.shell.find.Result;
import org.junit.Test;

public class TestNumberExpression extends TestExpression {

  @Test
  public void applyNumberEquals() throws IOException {
    NumberExpression numberExpr = new NumberExpression(){};
    addArgument(numberExpr, "5");
    numberExpr.initialise(new FindOptions());
    assertEquals(Result.PASS, numberExpr.applyNumber(5));
    assertEquals(Result.FAIL, numberExpr.applyNumber(4));
    assertEquals(Result.FAIL, numberExpr.applyNumber(6));
  }

  @Test
  public void applyNumberGreaterThan() throws IOException {
    NumberExpression numberExpr = new NumberExpression(){};
    addArgument(numberExpr, "+5");
    numberExpr.initialise(new FindOptions());
    assertEquals(Result.FAIL, numberExpr.applyNumber(5));
    assertEquals(Result.FAIL, numberExpr.applyNumber(4));
    assertEquals(Result.PASS, numberExpr.applyNumber(6));
  }

  @Test
  public void applyNumberLessThan() throws IOException {
    NumberExpression numberExpr = new NumberExpression(){};
    addArgument(numberExpr, "-5");
    numberExpr.initialise(new FindOptions());
    assertEquals(Result.FAIL, numberExpr.applyNumber(5));
    assertEquals(Result.PASS, numberExpr.applyNumber(4));
    assertEquals(Result.FAIL, numberExpr.applyNumber(6));
  }
}
