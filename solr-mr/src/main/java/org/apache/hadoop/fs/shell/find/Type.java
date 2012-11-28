package org.apache.hadoop.fs.shell.find;

import java.io.IOException;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.shell.PathData;

/**
 * Implements the -type expression for the {@link org.apache.hadoop.fs.shell.find.Find} command.
 */
public final class Type extends BaseExpression {
  private static final String[] USAGE = {
    "-type filetype"
  };
  private static final String[] HELP = {
    "Evaluates to true if the file type matches that specified.",
    "The following file type values are supported:",
    "'d' (directory), 'l' (symbolic link), 'f' (regular file)."
  };

  private static final FileType DIRECTORY;
  private static final FileType SYMBOLIC_LINK;
  private static final FileType REGULAR_FILE;

  public static final Map<String,FileType> FILE_TYPES;

  public Type() {
    super();
    setUsage(USAGE);
    setHelp(HELP);
  }

  private static abstract class FileType {
    private String code;
    public FileType(String code) {
      this.code = code;
    }
    public String getCode() {
      return this.code;
    }
    public abstract boolean matches(FileStatus stat);
  }
  static {
    DIRECTORY = new FileType("d") {
      public boolean matches(FileStatus stat) {
        return stat.isDirectory();
      }
    };
    SYMBOLIC_LINK = new FileType("l") {
      public boolean matches(FileStatus stat) {
        return stat.isSymlink();
      }
    };
    REGULAR_FILE = new FileType("f") {
      public boolean matches(FileStatus stat) {
        return stat.isFile();
      }
    };
    
    HashMap<String,FileType> map = new HashMap<String,FileType>();
    map.put(DIRECTORY.getCode(), DIRECTORY);
    map.put(SYMBOLIC_LINK.getCode(), SYMBOLIC_LINK);
    map.put(REGULAR_FILE.getCode(), REGULAR_FILE);
    FILE_TYPES = Collections.unmodifiableMap(map);
  }
  
  private FileType fileType = null;

  /** {@inheritDoc} */
  @Override
  public void addArguments(Deque<String> args) {
    addArguments(args, 1);
  }
  
  /** {@inheritDoc} */
  @Override
  public void initialise(FindOptions option) throws IOException {
    String arg = getArgument(1);
    if(FILE_TYPES.containsKey(arg)) {
      this.fileType = FILE_TYPES.get(arg);
    }
    else {
      throw new IOException("Invalid file type: " + arg);
    }
  }
  
  /** {@inheritDoc} */
  @Override
  public Result apply(PathData item) throws IOException {
    if(this.fileType.matches(getFileStatus(item))) {
      return Result.PASS;
    }
    return Result.FAIL;
  }
  /** Registers this expression with the specified factory. */
  public static void registerExpression(ExpressionFactory factory) throws IOException {
    factory.addClass(Type.class, "-type");
  }
}
