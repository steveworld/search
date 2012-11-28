package org.apache.hadoop.fs.shell.find;

import java.io.IOException;

import org.apache.hadoop.fs.shell.PathData;

/**
 * Implements the -blocksize expression for the {@link org.apache.hadoop.fs.shell.find.Find} command.
 */
public final class Blocksize extends NumberExpression {
  private static final String[] USAGE = {
    "-blocks n"
  };
  private static final String[] HELP = {
    "Evaluates to true if the number of file blocks is n.",
  };

  public Blocksize() {
    super();
    setUsage(USAGE);
    setHelp(HELP);
  }

  /** {@inheritDoc} */
  @Override
  public Result apply(PathData item) throws IOException {
    return applyNumber(getFileStatus(item).getBlockSize());
  }
  /** Registers this expression with the specified factory. */
  public static void registerExpression(ExpressionFactory factory) throws IOException {
    factory.addClass(Blocksize.class, "-blocksize");
  }
}
