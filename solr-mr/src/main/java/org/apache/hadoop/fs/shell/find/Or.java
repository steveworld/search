package org.apache.hadoop.fs.shell.find;

import java.io.IOException;
import java.util.Deque;

import org.apache.hadoop.fs.shell.PathData;

/**
 * Implements the -o (or) operator for the {@link org.apache.hadoop.fs.shell.find.Find} command.
 */
public final class Or extends BaseExpression {
  private static final String[] USAGE = {
    "expression -o expression",
    "expression -or expression"
  };
  private static final String[] HELP = {
    "Logical OR operator for joining two expressions. Returns",
    "true if one of the child expressions returns true. The",
    "second expression will not be applied if the first returns",
    "true."
  };

  public Or() {
    super();
    setUsage(USAGE);
    setHelp(HELP);
  }

  /** {@inheritDoc} */
  @Override
  public Result apply(PathData item) throws IOException {
    for(Expression child : getChildren()) {
      Result result = child.apply(item);
      if(result.isPass()) {
        return result;
      }
    }
    return Result.FAIL;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isOperator() {
    return true;
  }

  /** {@inheritDoc} */
  @Override
  public int getPrecedence() {
    return 100;
  }
  
  /** {@inheritDoc} */
  @Override
  public void addChildren(Deque<Expression> expressions) {
    addChildren(expressions, 2);
  }
  /** Registers this expression with the specified factory. */
  public static void registerExpression(ExpressionFactory factory) throws IOException {
    factory.addClass(Or.class, "-o");
    factory.addClass(Or.class, "-or");
  }
}
