package org.apache.hadoop.fs.shell.find;

import java.io.IOException;

import org.apache.hadoop.fs.shell.PathData;

/**
 * Implements the -nogroup expression for the {@link org.apache.hadoop.fs.shell.find.Find} command.
 */
public final class Nogroup extends BaseExpression {
  private static final String[] USAGE = {
    "-nogroup"
  };
  private static final String[] HELP = {
    "Evaluates as true if the file does not have a valid group."
  };

  public Nogroup() {
    super();
    setUsage(USAGE);
    setHelp(HELP);
  }

  /** {@inheritDoc} */
  @Override
  public Result apply(PathData item) throws IOException {
    String group = getFileStatus(item).getGroup();
    if((group == null) || (group.equals(""))) {
      return Result.PASS;
    }
    return Result.FAIL;
  }
  /** Registers this expression with the specified factory. */
  public static void registerExpression(ExpressionFactory factory) throws IOException {
    factory.addClass(Nogroup.class, "-nogroup");
  }
}
