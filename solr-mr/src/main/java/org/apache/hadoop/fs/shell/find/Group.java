package org.apache.hadoop.fs.shell.find;

import java.io.IOException;
import java.util.Deque;

import org.apache.hadoop.fs.shell.PathData;

/**
 * Implements the -group expression for the {@link org.apache.hadoop.fs.shell.find.Find} command.
 */
public final class Group extends BaseExpression {
  private static final String[] USAGE = {
    "-group groupname"
  };
  private static final String[] HELP = {
    "Evaluates as true if the file belongs to the specified",
    "group."
  };
  private String requiredGroup;
  
  public Group() {
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
    requiredGroup = getArgument(1);
  }
  
  /** {@inheritDoc} */
  @Override
  public Result apply(PathData item) throws IOException {
    String group = getFileStatus(item).getGroup();
    if(requiredGroup.equals(group)) {
      return Result.PASS;
    }
    return Result.FAIL;
  }
  /** Registers this expression with the specified factory. */
  public static void registerExpression(ExpressionFactory factory) throws IOException {
    factory.addClass(Group.class, "-group");
  }
}
