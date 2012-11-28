package org.apache.hadoop.fs.shell.find;

import java.io.IOException;
import java.util.Deque;

/**
 * Implements the -class expression for the {@link org.apache.hadoop.fs.shell.find.Find} command.
 */
public final class ClassExpression extends FilterExpression {
  private static final String[] USAGE = {
    "-class classname [args ...]"
  };
  private static final String[] HELP = {
    "Executes the named expression class."
  };
  /** Registers this expression with the specified factory. */
  public static void registerExpression(ExpressionFactory factory) throws IOException {
    factory.addClass(ClassExpression.class, "-class");
  }

  public ClassExpression() {
    super(null);
  }
  @Override
  public void addArguments(Deque<String> args) {
    String classname = args.pop();
    expression = ExpressionFactory.getExpressionFactory().createExpression(classname, getConf());
    super.addArguments(args);
  }

  /** {@inheritDoc} */
  @Override
  public String[] getUsage() {
    return USAGE;
  }
  
  /** {@inheritDoc} */
  @Override
  public String[] getHelp() {
    return HELP;
  }

  /** {@inheritDoc} */
  @Override
  public boolean isOperator() {
    if(expression == null) {
      return false;
    }
    return expression.isOperator();
  }
}
