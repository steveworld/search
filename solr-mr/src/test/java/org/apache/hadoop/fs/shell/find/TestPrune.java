package org.apache.hadoop.fs.shell.find;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

import java.io.IOException;

import org.apache.hadoop.fs.shell.PathData;
import org.apache.hadoop.fs.shell.find.FindOptions;
import org.apache.hadoop.fs.shell.find.Prune;
import org.apache.hadoop.fs.shell.find.Result;
import org.junit.Test;

public class TestPrune extends TestExpression {

  @Test
  public void apply() throws IOException {
    Prune prune = new Prune();
    PathData item = mock(PathData.class);
    Result result = prune.apply(item);
    assertTrue(result.isPass());
    assertFalse(result.isDescend());
  }

  // check that the prune command is ignore when doing a depth first find
  @Test
  public void applyDepth() throws IOException {
    Prune prune = new Prune();
    FindOptions options = new FindOptions();
    options.setDepth(true);
    prune.initialise(options);
    PathData item = mock(PathData.class);
    assertEquals(Result.PASS, prune.apply(item));
  }
}
