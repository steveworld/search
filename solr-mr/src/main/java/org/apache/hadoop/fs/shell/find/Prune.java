package org.apache.hadoop.fs.shell.find;

import java.io.IOException;

import org.apache.hadoop.fs.shell.PathData;

/**
 * Implements -prune expression for the {@link org.apache.hadoop.fs.shell.find.Find} command.
 */
public final class Prune extends BaseExpression {
  private static final String[] USAGE = {
    "-prune"
  };
  private static final String[] HELP = {
    "Always evaluates to true. Causes the find command to not",
    "descend any further down this directory tree. Does not",
    "have any affect if the -depth expression is specified."
  };

  public Prune() {
    super();
    setUsage(USAGE);
    setHelp(HELP);
  }

  @Override
  public Result apply(PathData item) throws IOException {
    if(getOptions().isDepth()) {
      return Result.PASS;
    }
    return Result.STOP;
  }
  /** Registers this expression with the specified factory. */
  public static void registerExpression(ExpressionFactory factory) throws IOException {
    factory.addClass(Prune.class, "-prune");
  }
}
