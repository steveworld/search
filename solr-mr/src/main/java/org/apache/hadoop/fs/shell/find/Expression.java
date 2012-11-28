package org.apache.hadoop.fs.shell.find;

import java.io.IOException;
import java.util.Deque;

import org.apache.hadoop.fs.shell.PathData;

/**
 * Interface describing an expression to be used in the {@link org.apache.hadoop.fs.shell.find.Find} command.
 */
public interface Expression {
  /**
   * Initialise and validate this expression, called once before processing any items.
   */
  public void initialise(FindOptions options) throws IOException;
  /**
   * Apply the expression to the specified item, called once for each item.
   * @param item
   * @return {@link Result} of applying the expression to the item
   */
  public Result apply(PathData item) throws IOException;
  /**
   * Finishes the expression, called once after processing all items.
   * @throws IOException
   */
  public void finish() throws IOException;

  /**
   * Returns brief usage instructions for this expression.
   * Multiple items should be returned if there are multiple ways to use this expression.
   * @return array of usage instructions
   */
  public String[] getUsage();

  /**
   * Returns a description of the expression for use in help.
   * Multiple lines should be returned array items.
   * Lines should be formated to 60 characters or less.
   * @return array of description lines
   */
  public String[] getHelp();
  
  /** Indicates whether this expression performs an action, i.e. provides output back to the user. */
  public boolean isAction();

  /** Identifies the expression as an operator rather than a primary. */
  public boolean isOperator();
  
  /** Returns the precendence of this expression (only applicable to operators). */
  public int getPrecedence();
  
  /**
   * Adds children to this expression.  Children are popped from the head of the deque.
   * @param expressions deque of expressions from which to take the children
   */
  public void addChildren(Deque<Expression> expressions);
  
  /**
   * Adds arguments to this expression.
   * Arguments are popped from the head of the deque and added to the front of
   * the child list, ie last child added is the first evaluated.
   * @param args deque of arguments from which to take expression arguments
   */
  public void addArguments(Deque<String> args);
}
