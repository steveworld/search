package org.apache.hadoop.fs.shell.find;

import java.io.IOException;

import org.apache.hadoop.fs.shell.PathData;

/**
 * Implements the -replicas expression for the {@link org.apache.hadoop.fs.shell.find.Find} command.
 */
public final class Replicas extends NumberExpression {
  private static final String[] USAGE = {
    "-replicas n"
  };
  private static final String[] HELP = {
    "Evaluates to true if the number of file replicas is n.",
  };

  public Replicas() {
    super();
    setUsage(USAGE);
    setHelp(HELP);
  }

  /** {@inheritDoc} */
  @Override
  public Result apply(PathData item) throws IOException {
    return applyNumber(getFileStatus(item).getReplication());
  }
  /** Registers this expression with the specified factory. */
  public static void registerExpression(ExpressionFactory factory) throws IOException {
    factory.addClass(Replicas.class, "-replicas");
  }
}
