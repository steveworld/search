/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.hadoop.fs.shell.find;

import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;

public class MockFileSystem extends FilterFileSystem {
  private static Configuration conf;
  private static HashMap<String,FileStatus> fileStatusMap;
  private static HashMap<String,FileStatus[]> listStatusMap;
  private static HashMap<String,FileStatus[]> globStatusMap;
  private static FileStatus fileStatus;

  public static void reset() {
    conf = new Configuration();
    conf.set("fs.defaultFS", "mockfs:///");
    conf.setClass("fs.mockfs.impl", MockFileSystem.class, FileSystem.class);
    fileStatusMap = new HashMap<String,FileStatus>();
    listStatusMap = new HashMap<String,FileStatus[]>();
    globStatusMap = new HashMap<String,FileStatus[]>();
    fileStatus = null;
  }
  public MockFileSystem(FileSystem fs) {
    super(fs);
  }
  public MockFileSystem() {
    this(mock(FileSystem.class));
  }
  
  @Override
  public void initialize(URI uri, Configuration conf) {
    MockFileSystem.conf = conf;
  }
  @Override
  public Path makeQualified(Path path) {
    return path;
  }
  @Override
  public Configuration getConf() {
    return conf;
  }
  
  /** Set a default file status to be returned by the file system. */
  public void setFileStatus(FileStatus fstat) {
    fileStatus = fstat; 
  }
  
  /** Set a file status to be returned for a given file name. */
  public void setFileStatus(String name, FileStatus fileStatus) {
    fileStatusMap.put(name, fileStatus);
  }
  
  public void setListStatus(String name, FileStatus[] listStatus) {
    listStatusMap.put(name, listStatus);
  }

  public void setGlobStatus(String name, FileStatus[] listStatus) {
    globStatusMap.put(name, listStatus);
  }

  /**
   * Returns the file status for the give path.
   * If a file status has been set for this path name (using setFileStatus) then that is returned.
   * Else if a default file status has been set then that is returned.
   * Else the value from the base file system is returned. 
   */
  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    if(fileStatusMap.containsKey(path.getName())) {
      return fileStatusMap.get(path.getName());
    }
    if(fileStatus != null) {
      return fileStatus;
    }
    return super.getFileStatus(path);
  }
  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    if(listStatusMap.containsKey(path.getName())) {
      return listStatusMap.get(path.getName());
    }
    if(fileStatus != null) {
      return new FileStatus[]{fileStatus};
    }
    return super.listStatus(path);
  }
  
  public FileStatus[] globStatus(Path pathPattern) throws IOException {
    if(globStatusMap.containsKey(pathPattern.getName())) {
      return globStatusMap.get(pathPattern.getName());
    }
    if(fileStatus != null) {
      return new FileStatus[]{fileStatus};
    }
    return super.globStatus(pathPattern);
  }
  public Path getWorkingDirectory() {
    return new Path("/");
  }

}
