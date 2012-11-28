package org.apache.hadoop.fs.shell.find;

import java.io.IOException;
import java.util.Deque;

import org.apache.hadoop.fs.shell.PathData;

/**
 * Implements the ! (not) operator for the {@link org.apache.hadoop.fs.shell.find.Find} command.
 */
public final class Not extends BaseExpression {
  private static final String[] USAGE = {
    "! expression",
    "-not expression"
  };
  private static final String[] HELP = {
    "Evaluates as true if the expression evaluates as false and",
    "vice-versa."
  };

  public Not() {
    super();
    setUsage(USAGE);
    setHelp(HELP);
  }

  /** {@inheritDoc} */
  @Override
  public Result apply(PathData item) throws IOException {
    Expression child = getChildren().get(0);
    Result result = child.apply(item);
    return result.negate();
  }

  /** {@inheritDoc} */
  @Override
  public boolean isOperator() {
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public int getPrecedence() {
    return 300;
  }

  /** {@inheritDoc} */
  @Override
  public void addChildren(Deque<Expression> expressions) {
    addChildren(expressions, 1);
  }
  /** Registers this expression with the specified factory. */
  public static void registerExpression(ExpressionFactory factory) throws IOException {
    factory.addClass(Not.class, "!");
    factory.addClass(Not.class, "-not");
  }
}
