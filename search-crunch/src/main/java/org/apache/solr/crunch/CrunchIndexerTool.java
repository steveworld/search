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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.crunch.DoFn;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.PipelineResult.StageResult;
import org.apache.crunch.Source;
import org.apache.crunch.TableSource;
import org.apache.crunch.fn.FilterFns;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.impl.spark.SparkPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.avro.AvroFileSource;
import org.apache.crunch.io.impl.FileTableSourceImpl;
import org.apache.crunch.io.parquet.AvroParquetFileSource;
import org.apache.crunch.io.text.NLineFileSource;
import org.apache.crunch.lib.Sort;
import org.apache.crunch.types.avro.AvroInputFormat;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.solr.crunch.CrunchIndexerToolOptions.PipelineType;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import parquet.avro.AvroParquetInputFormat;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.io.Files;


/**
 * MapReduce ETL batch job driver that pipes data from an input source (splitable or non-splitable
 * HDFS files or Kite datasets) to an output target (HDFS files or Kite dataset or Apache Solr), and
 * along the way runs the data through a (optional) Morphline for extraction and transformation,
 * followed by a (optional) Crunch join followed by a (optional) arbitrary custom sequence of Crunch
 * processing steps.
 * 
 * The program is designed for flexible, scalable and fault-tolerant batch ETL pipeline jobs. It is
 * implemented as an Apache Crunch pipeline and as such can run on either the Apache Hadoop
 * MapReduce or Apache Spark execution engine.
 */
public class CrunchIndexerTool extends Configured implements Tool {

  @VisibleForTesting
  PipelineResult pipelineResult = null;
  
  private Path tmpFile = null;
  
  static final String MAIN_MEMORY_RANDOMIZATION_THRESHOLD =
      CrunchIndexerTool.class.getName() + ".mainMemoryRandomizationThreshold";
  
  /**
   * Morphline variables can be passed from CLI to the morphline, e.g.:
   * hadoop ... -D morphlineVariable.zkHost=127.0.0.1:2181/solr
   */
  static final String MORPHLINE_VARIABLE_PARAM = "morphlineVariable";

  private static final Logger LOG = LoggerFactory.getLogger(CrunchIndexerTool.class);

  /** API for command line clients */
  public static void main(String[] args) throws Exception  {
    int exitCode = ToolRunner.run(new Configuration(), new CrunchIndexerTool(), args);
    System.exit(exitCode);
  }

  public CrunchIndexerTool() {}

  @Override
  public int run(String[] args) throws Exception {
    CrunchIndexerToolOptions opts = new CrunchIndexerToolOptions();
    Integer exitCode = new CrunchIndexerToolArgumentParser().parseArgs(args, getConf(), opts);
    if (exitCode != null) {
      return exitCode;
    }
    return run(opts);
  }

  /** API for Java clients; visible for testing; may become a public API eventually */
  int run(CrunchIndexerToolOptions opts) throws Exception {
    long programStartTime = System.currentTimeMillis();

    if (opts.log4jConfigFile != null) {
      Utils.setLogConfigFile(opts.log4jConfigFile, getConf());
    }

    LOG.info("Initializing ...");

    int mappers = 1;
    Pipeline pipeline; // coordinates pipeline creation and execution
    if (opts.pipelineType == PipelineType.memory) {
      pipeline = MemPipeline.getInstance();
      pipeline.setConfiguration(getConf());
    } else if (opts.pipelineType == PipelineType.mapreduce) {
      pipeline = new MRPipeline(
          getClass(), 
          Utils.getShortClassName(getClass()), 
          getConf());

      mappers = new JobClient(pipeline.getConfiguration()).getClusterStatus().getMaxMapTasks(); // MR1
      //mappers = job.getCluster().getClusterStatus().getMapSlotCapacity(); // Yarn only
      LOG.info("Cluster reports {} mapper slots", mappers);
      
      int reducers = new JobClient(pipeline.getConfiguration()).getClusterStatus().getMaxReduceTasks(); // MR1
      //reducers = job.getCluster().getClusterStatus().getReduceSlotCapacity(); // Yarn only      
      LOG.info("Cluster reports {} reduce slots", reducers);
    } else if (opts.pipelineType == PipelineType.spark) {
      SparkConf sconf = new SparkConf();
      if (!sconf.contains("spark.app.name") || sconf.get("spark.app.name").equals(getClass().getName())) {
        sconf.setAppName(Utils.getShortClassName(getClass()));
      }
      JavaSparkContext sparkContext = new JavaSparkContext(sconf);
      pipeline = new SparkPipeline(sparkContext, sparkContext.appName());
      pipeline.setConfiguration(getConf());
    } else {
      throw new IllegalArgumentException("Unsupported --pipeline-type: " + opts.pipelineType);
    }

    this.tmpFile = null;    
    try {
      PCollection collection = extractInputCollection(opts, mappers, pipeline);
      if (collection == null) {
        return 0;
      }

      String morphlineFileContents = Files.toString(opts.morphlineFile, Charsets.UTF_8);
      Map<String, String> morphlineVariables = new HashMap<String, String>();
      for (Map.Entry<String, String> entry : pipeline.getConfiguration()) {
        String variablePrefix = MORPHLINE_VARIABLE_PARAM + ".";
        if (entry.getKey().startsWith(variablePrefix)) {
          morphlineVariables.put(entry.getKey().substring(variablePrefix.length()), entry.getValue());
        }
      }
      DoFn morphlineFn = new MorphlineFn(morphlineFileContents, opts.morphlineId, morphlineVariables);
      collection = collection.parallelDo(
          "morphline",
          morphlineFn, 
          Avros.nulls() // trick to enable morphline to emit any kind of output data, including non-avro data
          );
      
      collection = collection.parallelDo(
          FilterFns.REJECT_ALL(), // aka dropRecord
          Avros.nulls() // trick to enable morphline to emit any kind of output data, including non-avro data
          );
  
      writeOutput(opts, pipeline, collection);
  
      if (!done(pipeline, opts.isVerbose)) {
        return 1; // job failed
      }
      float secs = (System.currentTimeMillis() - programStartTime) / 1000.0f;
      LOG.info("Success. Done. Program took {} secs. Goodbye.", secs);
      return 0;
    } finally {
      // FIXME fails for yarn-cluster mode with spark unless hdfs permissions are fixed on tmp dir
      if (tmpFile != null) {
        FileSystem tmpFs = tmpFile.getFileSystem(pipeline.getConfiguration());
        delete(tmpFile, false, tmpFs);
        tmpFile = null;
      }
    }
  }

  private PCollection extractInputCollection(CrunchIndexerToolOptions opts, int mappers, Pipeline pipeline)
      throws IOException {
    
    if (opts.inputFiles.isEmpty() && opts.inputFileLists.isEmpty()) {
      return null;
    }
    
    // handle input files (not Kite datasets)
    tmpFile = new Path(
        pipeline.getConfiguration().get("hadoop.tmp.dir", "/tmp"), 
        getClass().getName() + "-" + UUID.randomUUID().toString());
    FileSystem tmpFs = tmpFile.getFileSystem(pipeline.getConfiguration());          
    LOG.debug("Creating list of input files for mappers: {}", tmpFile);
    long numFiles = addInputFiles(opts.inputFiles, opts.inputFileLists, tmpFile, pipeline.getConfiguration());
    if (numFiles == 0) {
      LOG.info("No input files found - nothing to process");
      return null;
    }
 
    if (opts.inputFileFormat != null) { // handle splitable input files
      LOG.info("Using these parameters: numFiles: {}", numFiles);
      List<Path> filePaths = new ArrayList<Path>();
      for (String file : listFiles(tmpFs, tmpFile)) {
        filePaths.add(new Path(file));
      }
      if (opts.inputFileFormat.isAssignableFrom(AvroInputFormat.class)) { 
        // hack that fixes IncompatibleClassChangeError
        Schema schema = opts.inputFileReaderSchema != null ? 
            opts.inputFileReaderSchema : getAvroSchemaFromPath(filePaths.get(0), new Configuration());
        Source source = new AvroFileSource(filePaths, Avros.generics(schema));
        return pipeline.read(source);
      } else if (opts.inputFileFormat.isAssignableFrom(AvroParquetInputFormat.class)) {
        if (opts.inputFileReaderSchema == null) {
          // TODO: for convenience we should extract the schema from the parquet data files. 
          // (i.e. we should do the same as above for avro files).
          throw new IllegalArgumentException(
              "--input-file-reader-schema must be specified when using --input-file-format=avroParquet");
        }
        Schema schema = opts.inputFileReaderSchema;
        Source source = new AvroParquetFileSource(filePaths, Avros.generics(schema), opts.inputFileProjectionSchema);
        return pipeline.read(source);
      } else if (opts.inputFileFormat.isAssignableFrom(TextInputFormat.class)) {
        Source source = From.textFile(filePaths);
        return pipeline.read(source);
      } else {
        // FIXME drop support for this stuff? (doesn't seem to work with spark)
        // TODO: intentionally restrict to only allow org.apache.hadoop.mapreduce.lib.input.TextInputFormat ?
        TableSource source = new FileTableSourceImpl(
            filePaths,
            WritableTypeFamily.getInstance().tableOf(Writables.longs(), Writables.strings()), 
            //AvroTypeFamily.getInstance().tableOf(Avros.nulls(), Avros.nulls()), 
            //AvroTypeFamily.getInstance().tableOf(Avros.longs(), Avros.generics(opts.inputFileSchema)), 
            opts.inputFileFormat);
        return pipeline.read(source).values();                 
      }
    } else { // handle non-splitable input files
      if (opts.mappers == -1) { 
        mappers = 8 * mappers; // better accomodate stragglers
      } else {
        mappers = opts.mappers;
      }
      if (mappers <= 0) {
        throw new IllegalStateException("Illegal number of mappers: " + mappers);
      }
      
      int numLinesPerSplit = (int) ceilDivide(numFiles, mappers);
      if (numLinesPerSplit < 0) { // numeric overflow from downcasting long to int?
        numLinesPerSplit = Integer.MAX_VALUE;
      }
      numLinesPerSplit = Math.max(1, numLinesPerSplit);

      int realMappers = Math.min(mappers, (int) ceilDivide(numFiles, numLinesPerSplit));
      LOG.info("Using these parameters: numFiles: {}, mappers: {}, realMappers: {}",
          new Object[] {numFiles, mappers, realMappers});

      boolean randomizeFewInputFiles = 
          numFiles < pipeline.getConfiguration().getInt(MAIN_MEMORY_RANDOMIZATION_THRESHOLD, 100001);
      if (randomizeFewInputFiles) {
        // If there are few input files reduce latency by directly running main memory randomization 
        // instead of launching a high latency MapReduce job
        randomizeFewInputFiles(tmpFs, tmpFile);
      }
 
      PCollection collection = pipeline.read(new NLineFileSource<String>(tmpFile, Writables.strings(), numLinesPerSplit));

      if (!randomizeFewInputFiles) {
        collection = randomize(collection); // uses a high latency MapReduce job
      }
      collection = collection.parallelDo(new HeartbeatFn(), collection.getPType());
      return collection;
    }
  }
  
  private void writeOutput(CrunchIndexerToolOptions opts, Pipeline pipeline, PCollection collection) {
    if (true) {
      // switch off Crunch lazy evaluation, yet don't write to a Crunch Target
      pipeline.materialize(collection);
//      for (Object item : pipeline.materialize(collection)) {
//        LOG.debug("item: {}", item);
//      }
//    } else {
//      throw new UnsupportedOperationException();
////      Target target = To.formattedFile(opts.outputDir, opts.outputFileFormat);
////      pipeline.write(collection, target, opts.outputWriteMode);
    }
  }
    
  private long addInputFiles(List<Path> inputFiles, List<Path> inputLists, 
      Path fullInputList, Configuration conf) throws IOException {
    
    long numFiles = 0;
    FileSystem fs = fullInputList.getFileSystem(conf);
    FSDataOutputStream out = fs.create(fullInputList);
    try {
      Writer writer = new BufferedWriter(new OutputStreamWriter(out, Charsets.UTF_8));
      
      for (Path inputFile : inputFiles) {
        FileSystem inputFileFs = inputFile.getFileSystem(conf);
        if (inputFileFs.exists(inputFile)) {
          PathFilter pathFilter = new PathFilter() {      
            @Override
            public boolean accept(Path path) { // ignore "hidden" files and dirs
              return !(path.getName().startsWith(".") || path.getName().startsWith("_")); 
            }
          };
          numFiles += addInputFilesRecursively(inputFile, writer, inputFileFs, pathFilter);
        }
      }

      for (Path inputList : inputLists) {
        InputStream in;
        if (inputList.toString().equals("-")) {
          in = System.in;
        } else if (inputList.isAbsoluteAndSchemeAuthorityNull()) {
          in = new BufferedInputStream(new FileInputStream(inputList.toString()));
        } else {
          in = inputList.getFileSystem(conf).open(inputList);
        }
        try {
          BufferedReader reader = new BufferedReader(new InputStreamReader(in, Charsets.UTF_8));
          String line;
          while ((line = reader.readLine()) != null) {
            writer.write(line + "\n");
            numFiles++;
          }
          reader.close();
        } finally {
          in.close();
        }
      }
      
      writer.close();
    } finally {
      out.close();
    }    
    return numFiles;
  }
  
  /**
   * Add the specified file to the input set, if path is a directory then
   * add the files contained therein.
   */
  private long addInputFilesRecursively(Path path, Writer writer, FileSystem fs, PathFilter pathFilter) throws IOException {
    long numFiles = 0;
    for (FileStatus stat : fs.listStatus(path, pathFilter)) {
      LOG.debug("Adding path {}", stat.getPath());
      if (stat.isDirectory()) {
        numFiles += addInputFilesRecursively(stat.getPath(), writer, fs, pathFilter);
      } else {
        writer.write(stat.getPath().toString() + "\n");
        numFiles++;
      }
    }
    return numFiles;
  }
  
  private List<String> listFiles(FileSystem fs, Path fullInputList) throws IOException {    
    List<String> lines = new ArrayList<String>();
    BufferedReader reader = new BufferedReader(
        new InputStreamReader(fs.open(fullInputList), Charsets.UTF_8));
    try {
      String line;
      while ((line = reader.readLine()) != null) {
        lines.add(line);
      }
    } finally {
      reader.close();
    }
    return lines;
  }

  private void randomizeFewInputFiles(FileSystem fs, Path fullInputList) throws IOException {    
    List<String> lines = listFiles(fs, fullInputList);
    
    Collections.shuffle(lines, new Random(421439783L)); // constant seed for reproducability
    
    FSDataOutputStream out = fs.create(fullInputList);
    Writer writer = new BufferedWriter(new OutputStreamWriter(out, Charsets.UTF_8));
    try {
      for (String line : lines) {
        writer.write(line + "\n");
      } 
    } finally {
      writer.close();
    }
  }

  /** Randomizes the order of the items in the collection via a MapReduce job */
  private static <T> PCollection<T> randomize(PCollection<T> items) {
    PTable<Long, T> table = items.by("randomize", new RandomizeFn<T>(), Writables.longs());
    table = Sort.sort(table, Sort.Order.ASCENDING);
    return table.values();
  }

  private boolean done(Pipeline job, boolean isVerbose) {
    if (isVerbose) {
      job.enableDebug();
      job.getConfiguration().setBoolean("crunch.log.job.progress", true); // see class RuntimeParameters
    }    
    String name = job.getName();
    LOG.debug("Running pipeline: " + name);
    pipelineResult = job.done();
    boolean success = pipelineResult.succeeded();
    if (success) {
      LOG.info("Succeeded with pipeline: " + name + " " + getJobInfo(pipelineResult, isVerbose));
    } else {
      LOG.error("Pipeline failed: " + name + " " + getJobInfo(pipelineResult, isVerbose));
    }
    return success;
  }

  private String getJobInfo(PipelineResult job, boolean isVerbose) {
    StringBuilder buf = new StringBuilder();
    for (StageResult stage : job.getStageResults()) {
      buf.append("\nstageId: " + stage.getStageId() + ", stageName: " + stage.getStageName());
      if (isVerbose) {
        buf.append(", counters: ");
        Map<String, Set<String>> sortedCounterMap = new TreeMap<String, Set<String>>(stage.getCounterNames());
        for (Map.Entry<String, Set<String>> entry : sortedCounterMap.entrySet()) {
          String groupName = entry.getKey();
          buf.append("\n" + groupName);
          Set<String> sortedCounterNames = new TreeSet<String>(entry.getValue());
          for (String counterName : sortedCounterNames) {
            buf.append("\n    " + counterName + " : " + stage.getCounterValue(groupName, counterName));
          }
        }
      }
    }
    return buf.toString();
  }
  
  private boolean delete(Path path, boolean recursive, FileSystem fs) throws IOException {
    boolean success = fs.delete(path, recursive);
    if (!success) {
      LOG.error("Cannot delete " + path);
    }
    return success;
  }

  // same as IntMath.divide(p, q, RoundingMode.CEILING)
  private long ceilDivide(long p, long q) {
    long result = p / q;
    if (p % q != 0) {
      result++;
    }
    return result;
  }
  
  private Schema getAvroSchemaFromPath(Path path, Configuration conf) {
    DataFileReader<GenericRecord> reader = null;
    try {
      FileSystem fs = FileSystem.get(conf);
      if (!fs.isFile(path)) {
        FileStatus[] fstat = fs.listStatus(path, new PathFilter() {
          @Override
          public boolean accept(Path path) {
            String name = path.getName();
            return !name.startsWith("_") && !name.startsWith(".");
          }
        });
        if (fstat == null || fstat.length == 0) {
          throw new IllegalArgumentException("No valid files found in directory: " + path);
        }
        path = fstat[0].getPath();
      }
      reader = new DataFileReader<GenericRecord>(new FsInput(path, conf), new GenericDatumReader<GenericRecord>());
      return reader.getSchema();
    } catch (IOException e) {
      throw new RuntimeException("Error reading schema from path: "  + path, e);
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          // ignored
        }
      }
    }
  }

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class RandomizeFn<T> extends MapFn<T, Long> {
    
    private transient Random prng;

    @Override
    public void initialize() {
      // create a good random seed, yet ensure deterministic PRNG sequence for easy reproducability
      long taskId = getContext().getTaskAttemptID().getTaskID().getId(); // taskId = 0, 1, ..., N
      prng = new Random(421439783L * (taskId + 1));
    }

    @Override
    public Long map(T input) {
      return prng.nextLong();
    }
  }

}
