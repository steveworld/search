package org.apache.hadoop.fs.shell.find;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;

import org.apache.hadoop.fs.shell.find.Expression;

/**
 * Test utilities for unit testing the {@link Expression}s
 */
abstract class TestExpression {
  protected void addArgument(Expression expr, String arg) {
    expr.addArguments(new LinkedList<String>(Collections.singletonList(arg)));
  }
  protected LinkedList<String> getArgs(String cmd) {
    return new LinkedList<String>(Arrays.asList(cmd.split(" ")));
  }
}
