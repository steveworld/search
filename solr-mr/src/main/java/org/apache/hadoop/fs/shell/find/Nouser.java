package org.apache.hadoop.fs.shell.find;

import java.io.IOException;

import org.apache.hadoop.fs.shell.PathData;

/**
 * Implements the -nouser expression for the {@link org.apache.hadoop.fs.shell.find.Find} command.
 */
public final class Nouser extends BaseExpression {
  private static final String[] USAGE = {
    "-nouser"
  };
  private static final String[] HELP = {
    "Evaluates as true if the file does not have a valid owner."
  };
  
  public Nouser() {
    super();
    setUsage(USAGE);
    setHelp(HELP);
  }

  /** {@inheritDoc} */
  @Override
  public Result apply(PathData item) throws IOException {
    String user = getFileStatus(item).getOwner();
    if((user == null) || (user.equals(""))) {
      return Result.PASS;
    }
    return Result.FAIL;
  }
  /** Registers this expression with the specified factory. */
  public static void registerExpression(ExpressionFactory factory) throws IOException {
    factory.addClass(Nouser.class, "-nouser");
  }
}
