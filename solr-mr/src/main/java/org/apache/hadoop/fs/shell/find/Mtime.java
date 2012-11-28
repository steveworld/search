package org.apache.hadoop.fs.shell.find;

import java.io.IOException;

import org.apache.hadoop.fs.shell.PathData;

/**
 * Implements the -mtime expression for the {@link org.apache.hadoop.fs.shell.find.Find} command.
 */
public final class Mtime extends NumberExpression {
  private static final String[] USAGE = {
    "-mtime n",
    "-mmin n"
  };
  private static final String[] HELP = {
    "Evaluates as true if the file modification time subtracted",
    "from the start time is n days (or minutes if -mmin is used)"
  };

  public Mtime() {
    this(DAY_IN_MILLISECONDS);
  }
  public Mtime(long units) {
    super(units);
    setUsage(USAGE);
    setHelp(HELP);
  }

  /** {@inheritDoc} */
  @Override
  public Result apply(PathData item) throws IOException {
    return applyNumber(getOptions().getStartTime() - getFileStatus(item).getModificationTime());
  }
  /** Registers this expression with the specified factory. */
  public static void registerExpression(ExpressionFactory factory) throws IOException {
    factory.addClass(Mtime.class, "-mtime");
    factory.addClass(Mmin.class, "-mmin");
  }
  
  /** Implement -mmin expression (similar to -mtime but in minutes). */
  public static class Mmin extends FilterExpression {
    public Mmin() {
      super(new Mtime(MINUTE_IN_MILLISECONDS));
    }
  }
}
