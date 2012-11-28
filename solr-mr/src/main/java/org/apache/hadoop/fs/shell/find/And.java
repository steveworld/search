package org.apache.hadoop.fs.shell.find;

import java.io.IOException;
import java.util.Deque;

import org.apache.hadoop.fs.shell.PathData;

/**
 * Implements the -a (and) operator for the {@link org.apache.hadoop.fs.shell.find.Find} command.
 */
public final class And extends BaseExpression {
  private static final String[] USAGE = {
    "expression -a expression",
    "expression -and expression",
    "expression expression"
  };
  private static final String[] HELP = {
    "Logical AND operator for joining two expressions. Returns",
    "true if both child expressions return true. Implied by the",
    "juxtaposition of two expressions and so does not need to be",
    "explicitly specified. The second expression will not be",
    "applied if the first fails."
  };
  
  public And() {
    super();
    setUsage(USAGE);
    setHelp(HELP);
  }

  /** {@inheritDoc} */
  @Override
  public Result apply(PathData item) throws IOException {
    Result result = Result.PASS;
    for(Expression child : getChildren()) {
      Result childResult = child.apply(item);
      result = result.combine(childResult);
      if(!result.isPass()) {
        return result;
      }
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isOperator() {
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public int getPrecedence() {
    return 200;
  }

  /** {@inheritDoc} */
  @Override
  public void addChildren(Deque<Expression> expressions) {
    addChildren(expressions, 2);
  }
  /** Registers this expression with the specified factory. */
  public static void registerExpression(ExpressionFactory factory) throws IOException {
    factory.addClass(And.class, "-a");
    factory.addClass(And.class, "-and");
  }
}
