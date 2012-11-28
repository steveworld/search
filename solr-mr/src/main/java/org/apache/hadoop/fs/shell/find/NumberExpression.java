package org.apache.hadoop.fs.shell.find;

import java.io.IOException;
import java.util.Deque;

public abstract class NumberExpression extends BaseExpression {
  protected static final long DAY_IN_MILLISECONDS = 86400000l;
  protected static final long MINUTE_IN_MILLISECONDS = 60000l;

  private long max = -1;
  private long min = -1;
  private long units = 1;
  
  protected NumberExpression(long units) {
    setUnits(units);
  }
  protected NumberExpression() {
    this(1);
  }
  protected void setUnits(long units) {
    this.units = units;
  }
  
  /** {@inheritDoc} */
  @Override
  public void initialise(FindOptions options) throws IOException {
    super.initialise(options);
    parseArgument(getArgument(1));
  }

  protected void parseArgument(String arg) throws IOException {
    if(arg == null) {
      throw new IOException("Invalid null argument");
    }
    else if(arg.equals("")) {
      throw new IOException("Invalid empty argument");
    }
    if(arg.startsWith("+")) {
      min = (Long.parseLong(arg.substring(1)) * units) + units;
    }
    else if(arg.startsWith("-"))
    {
      max = (Long.parseLong(arg.substring(1)) * units) - 1;
    }
    else {
      min = Long.parseLong(arg) * units;
      max = min + units - 1;
    }
  }
  
  /** {@inheritDoc} */
  @Override
  public void addArguments(Deque<String> args) {
    addArguments(args, 1);
  }

  protected Result applyNumber(long value) {
    if((min > -1) && (min > value)) {
      return Result.FAIL;
    }
    if((max > -1) && (max < value)) {
      return Result.FAIL;
    }
    return Result.PASS;
  }
}
