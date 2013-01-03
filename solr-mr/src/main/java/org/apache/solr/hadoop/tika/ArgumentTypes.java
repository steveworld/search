/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.solr.hadoop.tika;

import java.io.File;
import java.io.IOException;

import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.ArgumentType;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;

/**
 * Various covenient reusable helpers for validation of command line arguments.
 */
public class ArgumentTypes {

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  public static final class PositiveInteger implements ArgumentType<Integer> {

    @Override
    public Integer convert(ArgumentParser parser, Argument arg, String value) throws ArgumentParserException {
      try {
        int n = Integer.parseInt(value);
        if (n <= 0) {
          throw new ArgumentParserException(String.format("%d is not a positive integer", n), parser);
        }
        return n;
      } catch (NumberFormatException e) {
          throw new ArgumentParserException(e, parser);
      }
    }
  }

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  public static class FileArgumentType implements ArgumentType<File> {
    
    private boolean acceptSystemIn = false;
    private boolean verifyExists = false;
    private boolean verifyNotExists = false;
    private boolean verifyIsFile = false;
    private boolean verifyIsDirectory = false;
    private boolean verifyCanRead = false;
    private boolean verifyCanWrite = false;
    private boolean verifyCanWriteParent = false;
    private boolean verifyCanExecute = false;
    private boolean verifyIsAbsolute = false;

    public FileArgumentType() {}
    
    public FileArgumentType acceptSystemIn() {
      acceptSystemIn = true;
      return this;
    }
    
    public FileArgumentType verifyExists() {
      verifyExists = true;
      return this;
    }
    
    public FileArgumentType verifyNotExists() {
      verifyNotExists = true;
      return this;
    }
    
    public FileArgumentType verifyIsFile() {
      verifyIsFile = true;
      return this;
    }
    
    public FileArgumentType verifyIsDirectory() {
      verifyIsDirectory = true;
      return this;
    }
    
    public FileArgumentType verifyCanRead() {
      verifyCanRead = true;
      return this;
    }
    
    public FileArgumentType verifyCanWrite() {
      verifyCanWrite = true;
      return this;
    }
    
    public FileArgumentType verifyCanWriteParent() {
      verifyCanWriteParent = true;
      return this;
    }
    
    public FileArgumentType verifyCanExecute() {
      verifyCanExecute = true;
      return this;
    }
    
    public FileArgumentType verifyIsAbsolute() {
      verifyIsAbsolute = true;
      return this;
    }
    
    @Override
    public File convert(ArgumentParser parser, Argument arg, String value) throws ArgumentParserException {
      File file = new File(value);
      if (verifyExists && !isSystemIn(file)) {
        verifyExists(parser, file);
      }
      if (verifyNotExists && !isSystemIn(file)) {
        verifyNotExists(parser, file);
      }
      if (verifyIsFile && !isSystemIn(file)) {
        verifyIsFile(parser, file);
      }
      if (verifyIsDirectory && !isSystemIn(file)) {
        verifyIsDirectory(parser, file);
      }
      if (verifyCanRead && !isSystemIn(file)) {
        verifyCanRead(parser, file);
      }
      if (verifyCanWrite && !isSystemIn(file)) {
        verifyCanWrite(parser, file);
      }
      if (verifyCanWriteParent && !isSystemIn(file)) {
        verifyCanWriteParent(parser, file);
      }
      if (verifyCanExecute && !isSystemIn(file)) {
        verifyCanExecute(parser, file);
      }
      if (verifyIsAbsolute && !isSystemIn(file)) {
        verifyIsAbsolute(parser, file);
      }
      verifyCustom(parser, file);
      return file;
    }
    
    protected void verifyExists(ArgumentParser parser, File file) throws ArgumentParserException {
      if (!file.exists()) {
        throw new ArgumentParserException("File not found: " + file, parser);
      }
    }    
    
    protected void verifyNotExists(ArgumentParser parser, File file) throws ArgumentParserException {
      if (file.exists()) {
        throw new ArgumentParserException("File found: " + file, parser);
      }
    }    
    
    protected void verifyIsFile(ArgumentParser parser, File file) throws ArgumentParserException {
      if (!file.isFile()) {
        throw new ArgumentParserException("Not a file: " + file, parser);
      }
    }    
    
    protected void verifyIsDirectory(ArgumentParser parser, File file) throws ArgumentParserException {
      if (!file.isDirectory()) {
        throw new ArgumentParserException("Not a directory: " + file, parser);
      }
    }    
    
    protected void verifyCanRead(ArgumentParser parser, File file) throws ArgumentParserException {
      if (!file.canRead()) {
        throw new ArgumentParserException("Insufficient permissions to read file: " + file, parser);
      }
    }    
    
    protected void verifyCanWrite(ArgumentParser parser, File file) throws ArgumentParserException {
      if (!file.canWrite()) {
        throw new ArgumentParserException("Insufficient permissions to write file: " + file, parser);
      }
    }    
    
    protected void verifyCanWriteParent(ArgumentParser parser, File file) throws ArgumentParserException {
      File parent = file.getParentFile();
      if (parent == null || !parent.canWrite()) {
        throw new ArgumentParserException("Cannot write parent of file: " + file, parser);
      }
    }    
    
    protected void verifyCanExecute(ArgumentParser parser, File file) throws ArgumentParserException {
      if (!file.canExecute()) {
        throw new ArgumentParserException("Insufficient permissions to execute file: " + file, parser);
      }
    }    
    
    protected void verifyIsAbsolute(ArgumentParser parser, File file) throws ArgumentParserException {
      if (!file.isAbsolute()) {
        throw new ArgumentParserException("Not an absolute file: " + file, parser);
      }
    }    
    
    protected void verifyCustom(ArgumentParser parser, File file) throws ArgumentParserException {      
    }
    
    protected boolean isSystemIn(File file) {
      return acceptSystemIn && file.getPath().equals("-");
    }
    
  }

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  public static class PathArgumentType implements ArgumentType<Path> {
    
    private final FileSystem fs;
    private boolean acceptSystemIn = false;
    private boolean verifyExists = false;
    private boolean verifyNotExists = false;
    private boolean verifyIsFile = false;
    private boolean verifyIsDirectory = false;
    private boolean verifyCanRead = false;
    private boolean verifyCanWrite = false;
    private boolean verifyCanWriteParent = false;
    private boolean verifyCanExecute = false;
    private boolean verifyIsAbsolute = false;
    private String verifyScheme = null;

    public PathArgumentType(FileSystem fs) {
      this.fs = fs;
    }
    
    public PathArgumentType acceptSystemIn() {
      acceptSystemIn = true;
      return this;
    }
    
    public PathArgumentType verifyExists() {
      verifyExists = true;
      return this;
    }
    
    public PathArgumentType verifyNotExists() {
      verifyNotExists = true;
      return this;
    }
    
    public PathArgumentType verifyIsFile() {
      verifyIsFile = true;
      return this;
    }
    
    public PathArgumentType verifyIsDirectory() {
      verifyIsDirectory = true;
      return this;
    }
    
    public PathArgumentType verifyCanRead() {
      verifyCanRead = true;
      return this;
    }
    
    public PathArgumentType verifyCanWrite() {
      verifyCanWrite = true;
      return this;
    }
    
    public PathArgumentType verifyCanWriteParent() {
      verifyCanWriteParent = true;
      return this;
    }
    
    public PathArgumentType verifyCanExecute() {
      verifyCanExecute = true;
      return this;
    }
    
    public PathArgumentType verifyIsAbsolute() {
      verifyIsAbsolute = true;
      return this;
    }
    
    public PathArgumentType verifyScheme(String scheme) {
      verifyScheme = scheme;
      return this;
    }
    
    @Override
    public Path convert(ArgumentParser parser, Argument arg, String value) throws ArgumentParserException {
      Path file = new Path(value);
      try {
        if (verifyScheme != null && !isSystemIn(file)) {
          verifyScheme(parser, file);
        }        
        if (verifyIsAbsolute && !isSystemIn(file)) {
          verifyIsAbsolute(parser, file);
        }
        if (verifyExists && !isSystemIn(file)) {
          verifyExists(parser, file);
        }
        if (verifyNotExists && !isSystemIn(file)) {
          verifyNotExists(parser, file);
        }
        if (verifyIsFile && !isSystemIn(file)) {
          verifyIsFile(parser, file);
        }
        if (verifyIsDirectory && !isSystemIn(file)) {
          verifyIsDirectory(parser, file);
        }
        if (verifyCanRead && !isSystemIn(file)) {
          verifyCanRead(parser, file);
        }
        if (verifyCanWrite && !isSystemIn(file)) {
          verifyCanWrite(parser, file);
        }
        if (verifyCanWriteParent && !isSystemIn(file)) {
          verifyCanWriteParent(parser, file);
        }
        if (verifyCanExecute && !isSystemIn(file)) {
          verifyCanExecute(parser, file);
        }
      } catch (IOException e) {
        throw new ArgumentParserException(e, parser);
      }
      return file;
    }
    
    protected void verifyExists(ArgumentParser parser, Path file) throws ArgumentParserException, IOException {
      if (!fs.exists(file)) {
        throw new ArgumentParserException("File not found: " + file, parser);
      }
    }    
    
    protected void verifyNotExists(ArgumentParser parser, Path file) throws ArgumentParserException, IOException {
      if (fs.exists(file)) {
        throw new ArgumentParserException("File found: " + file, parser);
      }
    }    
    
    protected void verifyIsFile(ArgumentParser parser, Path file) throws ArgumentParserException, IOException {
      if (!fs.isFile(file)) {
        throw new ArgumentParserException("Not a file: " + file, parser);
      }
    }    
    
    protected void verifyIsDirectory(ArgumentParser parser, Path file) throws ArgumentParserException, IOException {
      if (!fs.isDirectory(file)) {
        throw new ArgumentParserException("Not a directory: " + file, parser);
      }
    }    
    
    protected void verifyCanRead(ArgumentParser parser, Path file) throws ArgumentParserException, IOException {
      verifyExists(parser, file);
      if (!fs.getFileStatus(file).getPermission().getUserAction().implies(FsAction.READ)) {
        throw new ArgumentParserException("Insufficient permissions to read file: " + file, parser);
      }
    }    
    
    protected void verifyCanWrite(ArgumentParser parser, Path file) throws ArgumentParserException, IOException {
      verifyExists(parser, file);
      if (!fs.getFileStatus(file).getPermission().getUserAction().implies(FsAction.WRITE)) {
        throw new ArgumentParserException("Insufficient permissions to write file: " + file, parser);
      }
    }    
    
    protected void verifyCanWriteParent(ArgumentParser parser, Path file) throws ArgumentParserException, IOException {
      Path parent = file.getParent();
      if (parent == null || !fs.exists(parent) || !fs.getFileStatus(parent).getPermission().getUserAction().implies(FsAction.WRITE)) {
        throw new ArgumentParserException("Cannot write parent of file: " + file, parser);
      }
    }    
    
    protected void verifyCanExecute(ArgumentParser parser, Path file) throws ArgumentParserException, IOException {
      verifyExists(parser, file);
      if (!fs.getFileStatus(file).getPermission().getUserAction().implies(FsAction.EXECUTE)) {
        throw new ArgumentParserException("Insufficient permissions to execute file: " + file, parser);
      }
    }    
    
    protected void verifyIsAbsolute(ArgumentParser parser, Path file) throws ArgumentParserException, IOException {
      if (!file.isAbsolute()) {
        throw new ArgumentParserException("Not an absolute file: " + file, parser);
      }
    }    
    
    protected void verifyScheme(ArgumentParser parser, Path file) throws ArgumentParserException, IOException {
      if (!verifyScheme.equals(file.toUri().getScheme())) {
        throw new ArgumentParserException("Scheme of path: " + file + " must be: " + verifyScheme, parser);
      }
    }

    protected boolean isSystemIn(Path file) {
      return acceptSystemIn && file.toString().equals("-");
    }
    
  }

}
