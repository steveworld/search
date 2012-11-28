package org.apache.hadoop.fs.shell.find;

import java.io.IOException;
import java.util.Deque;

import org.apache.hadoop.fs.shell.PathData;

/**
 * Implements the -user expression for the {@link org.apache.hadoop.fs.shell.find.Find} command.
 */
public final class User extends BaseExpression {
  private static final String[] USAGE = {
    "-user username"
  };
  private static final String[] HELP = {
    "Evaluates as true if the owner of the file matches the",
    "specified user."
  };
  private String requiredUser;

  public User() {
    super();
    setUsage(USAGE);
    setHelp(HELP);
  }
  
  /** {@inheritDoc} */
  @Override
  public void addArguments(Deque<String> args) {
    addArguments(args, 1);
  }
  
  @Override
  public void initialise(FindOptions options) throws IOException {
    requiredUser = getArgument(1);
  }
  
  /** {@inheritDoc} */
  public Result apply(PathData item) throws IOException {
    if(requiredUser.equals(getFileStatus(item).getOwner())) {
      return Result.PASS;
    }
    return Result.FAIL;
  }

  /** Registers this expression with the specified factory. */
  public static void registerExpression(ExpressionFactory factory) throws IOException {
    factory.addClass(User.class, "-user");
  }
}
