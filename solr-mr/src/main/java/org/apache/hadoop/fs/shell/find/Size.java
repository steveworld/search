package org.apache.hadoop.fs.shell.find;

import java.io.IOException;

import org.apache.hadoop.fs.shell.PathData;

/**
 * Implements the -size expression for the {@link org.apache.hadoop.fs.shell.find.Find} command.
 */
public final class Size extends NumberExpression {
  private static final String[] USAGE = {
    "-size n[c]"
  };
  private static final String[] HELP = {
    "Evaluates to true if the file size in 512 byte blocks is n.",
    "If n is followed by the character 'c' then the size is in bytes."
  };

  public Size() {
    setUsage(USAGE);
    setHelp(HELP);
  }

  @Override
  protected void parseArgument(String arg) throws IOException {
    if(arg.endsWith("c")) {
      arg = arg.substring(0, arg.length() - 1);
    }
    else {
      setUnits(512);
    }
    super.parseArgument(arg);
  }
  
  /** {@inheritDoc} */
  @Override
  public Result apply(PathData item) throws IOException {
    return applyNumber(getFileStatus(item).getLen());
  }
  /** Registers this expression with the specified factory. */
  public static void registerExpression(ExpressionFactory factory) throws IOException {
    factory.addClass(Size.class, "-size");
  }
}
