package org.apache.hadoop.fs.shell.find;

import java.io.IOException;

import org.apache.hadoop.fs.shell.PathData;

/**
 * Implements the -depth expression for the {@link org.apache.hadoop.fs.shell.find.Find} command.
 */
public final class Depth extends BaseExpression {
  private static final String[] USAGE = {
    "-depth"
  };
  private static final String[] HELP = {
    "Always evaluates to true. Causes directory contents to be",
    "evaluated before the directory itself."
  };
  
  public Depth() {
    super();
    setUsage(USAGE);
    setHelp(HELP);
  }

  @Override
  public Result apply(PathData item) {
    return Result.PASS;
  }
  @Override
  public void initialise(FindOptions options) {
    options.setDepth(true);
  }
  /** Registers this expression with the specified factory. */
  public static void registerExpression(ExpressionFactory factory) throws IOException {
    factory.addClass(Depth.class, "-depth");
  }
}
