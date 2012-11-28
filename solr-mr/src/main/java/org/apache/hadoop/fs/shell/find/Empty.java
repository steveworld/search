package org.apache.hadoop.fs.shell.find;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.shell.PathData;

/**
 * Implements the -empty expression for the {@link org.apache.hadoop.fs.shell.find.Find} command.
 */
public final class Empty extends BaseExpression {
  private static final String[] USAGE = {
    "-empty"
  };
  private static final String[] HELP = {
    "Evaluates as true if the file is empty or directory has no",
    "contents."
  };

  public Empty() {
    super();
    setUsage(USAGE);
    setHelp(HELP);
  }

  /** {@inheritDoc} */
  @Override
  public Result apply(PathData item) throws IOException {
    FileStatus fileStatus = getFileStatus(item);
    if(fileStatus.isDirectory()) {
      if(getFileSystem(item).listStatus(getPath(item)).length == 0) {
        return Result.PASS;
      }
      return Result.FAIL;
    }
    if(fileStatus.getLen() == 0l) {
      return Result.PASS;
    }
    return Result.FAIL;
  }
  /** Registers this expression with the specified factory. */
  public static void registerExpression(ExpressionFactory factory) throws IOException {
    factory.addClass(Empty.class, "-empty");
  }
}
