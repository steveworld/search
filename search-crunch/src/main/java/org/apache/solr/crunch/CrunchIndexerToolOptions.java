/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.crunch;

import java.io.File;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


/**
 * Parsed command line options. These are produced by {@link CrunchIndexerToolArgumentParser} and
 * consumed by {@link CrunchIndexerTool}.
 */
final class CrunchIndexerToolOptions {

  List<Path> inputFileLists;
  List<Path> inputFiles;
  Class<FileInputFormat> inputFileFormat; // e.g. TextInputFormat
  Schema inputFileReaderSchema;
  Schema inputFileProjectionSchema;
  File morphlineFile;
  String morphlineId;
  PipelineType pipelineType;
  boolean isDryRun;
  File log4jConfigFile;
  boolean isVerbose;
  int mappers;

  public CrunchIndexerToolOptions() {}
    
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  static enum PipelineType {
    memory,
    mapreduce,
    spark
  }
    
}
