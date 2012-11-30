/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.hadoop.tika;


import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.solr.hadoop.IdentityMapper;
import org.apache.solr.hadoop.IdentityReducer;
import org.apache.solr.hadoop.LineRandomizerMapper;
import org.apache.solr.hadoop.LineRandomizerReducer;
import org.apache.solr.hadoop.SolrDocumentConverter;
import org.apache.solr.hadoop.SolrOutputFormat;
import org.apache.solr.hadoop.SolrReducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TikaIndexerTool extends Configured implements Tool {
  
  private Job job;
  
  public static final String RESULTS_DIR = "results";

  /** A list of input file URLs. Used as input to the Mapper */
  private static final String FULL_INPUT_LIST = "full-input-list.txt";

  private static final Logger LOG = LoggerFactory.getLogger(TikaIndexerTool.class);


  private void usage() {
    String msg = 
        "Usage: hadoop jar solr-mr.jar " + getClass().getName() + " [options]... [inputFileOrDir]... " +
        "\n" + 
        "\nSpecific options supported are" +
        "\n\n--outputdir <hdfsOutputDir>\tHDFS output directory containing Solr indexes (defaults to hdfs:///user/" + System.getProperty("user.name") + "/tikaindexer-output)" +
        "\n\n--inputlist <File>\tLocal file URL or HDFS file URL containing a list of HDFS URLs, one URL per line. If '-' is specified, URLs are read from the standard input." +
        "\n\n--solr <solrHome>\tLocal dir containing Solr conf/ and lib/ (defaults to " + System.getProperty("user.home") + File.separator + "solr)" +
        "\n\n--mappers <NNN>\tMaximum number of mappers to use (defaults to 1)" +
        "\n\n--shards NNN\tNumber of output shards (defaults to 1)" +
        "\n\n--verbose true|false\t (defaults to false)" +
        "\n";
    System.out.println(msg);
    ToolRunner.printGenericCommandUsage(System.out);
  }

  public static void main(String[] args) throws Exception  {
    Configuration conf = new Configuration();
    int res = ToolRunner.run(conf, new TikaIndexerTool(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length == 0) {
      usage();
      return -1;
    }
    
    job = Job.getInstance(getConf());
    job.setJarByClass(TikaIndexerTool.class);

    int shards = 1;
    int mappers = 1;
    File solrHomeDir = new File(System.getProperty("user.home") + File.separator + "solr");
    Path outputDir = new Path("hdfs:///user/" + System.getProperty("user.name") + "/tikaindexertool-output");
    List<String> inputLists = new ArrayList();
    List<Path> inputFiles = new ArrayList();
    boolean isRandomize = true;
    boolean isVerbose = false;
    boolean isIdentityTest = false;

    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("--outputdir")) {
        outputDir = new Path(args[++i]);
        checkHdfsPath(outputDir);
      } else if (args[i].equals("--inputlist")) {
        inputLists.add(args[++i]);
      } else if (args[i].equals("--solr")) {
        solrHomeDir = new File(args[++i]);
      } else if (args[i].equals("--shards")) {
        shards = Integer.parseInt(args[++i]);
      } else if (args[i].equals("--mappers")) {
        mappers = Integer.parseInt(args[++i]);
      } else if (args[i].equals("--verbose")) {
        isVerbose = true;
      } else if (args[i].equals("--norandomize")) {
        isRandomize = false;
      } else if (args[i].equals("--identitytest")) {
        isIdentityTest = true;
      } else {
        Path path = new Path(args[i]);
        checkHdfsPath(path);
        inputFiles.add(path);
      }
    }
    
    if (!solrHomeDir.exists()) {
      throw new IOException("Solr home directory does not exist: " + solrHomeDir);
    }

    FileSystem fs = FileSystem.get(job.getConfiguration());
    fs.delete(outputDir, true);    
    Path outputResultsDir = new Path(outputDir, RESULTS_DIR);    
    Path outputStep1Dir = new Path(outputDir, "tmp1");    
    Path outputStep2Dir = new Path(outputDir, "tmp2");    
    Path fullInputList = new Path(outputStep1Dir, FULL_INPUT_LIST);
    
    LOG.info("Creating full input list file for solr mappers {}", fullInputList);
    long numFiles = addInputFiles(inputFiles, inputLists, fullInputList);
    if (numFiles == 0) {
      throw new IOException("No input files found");
    }
    int numLinesPerSplit = (int) (numFiles / mappers);
    if (numLinesPerSplit < 0) { // numeric overflow from downcasting long to int?
      numLinesPerSplit = Integer.MAX_VALUE;
    }
    numLinesPerSplit = Math.max(1, numLinesPerSplit);

    if (isRandomize) { 
      Job randomizerJob = randomizeInputFiles(fullInputList, outputStep2Dir, numLinesPerSplit);
      if (!randomizerJob.waitForCompletion(isVerbose)) {
        return -1; // job failed
      }
    } else {
      outputStep2Dir = outputStep1Dir;
    }
    
    job.setInputFormatClass(NLineInputFormat.class);
    NLineInputFormat.addInputPath(job, outputStep2Dir);
    NLineInputFormat.setNumLinesPerSplit(job, numLinesPerSplit);    
    FileOutputFormat.setOutputPath(job, outputResultsDir);

    if (isIdentityTest) {
      job.setMapperClass(IdentityMapper.class);
      job.setReducerClass(IdentityReducer.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      job.setNumReduceTasks(1);  
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(NullWritable.class);
    } else {
      LOG.info("Indexing files...");
      job.setMapperClass(TikaMapper.class);
      job.setReducerClass(SolrReducer.class);
      job.setOutputFormatClass(SolrOutputFormat.class);
      SolrOutputFormat.setupSolrHomeCache(solrHomeDir, job);  
      job.setNumReduceTasks(shards);  
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(MapWritable.class);
      SolrDocumentConverter.setSolrDocumentConverter(TikaDocumentConverter.class, job.getConfiguration());
    }

    return job.waitForCompletion(isVerbose) ? 0 : -1;
  }

  /**
   * To uniformly spread load across all mappers we randomize fullInputList
   * with a separate small Mapper & Reducer preprocessing step. This way
   * each input line ends up on a random position in the output file list.
   * Each mapper indexes a disjoint consecutive set of files such that each
   * set has roughly the same size, at least from a probabilistic
   * perspective.
   * 
   * For example an input file with the following input list of URLs:
   * 
   * A
   * B
   * C
   * D
   * 
   * might be randomized into the following output list of URLS:
   * 
   * C
   * A
   * D
   * B
   */
  private Job randomizeInputFiles(Path fullInputList, Path outputStep2Dir, int numLinesPerSplit) throws IOException {
    LOG.info("Randomizing full input list file for solr mappers {}", fullInputList);
    Job job2 = Job.getInstance(new Configuration(getConf()));
    job2.setJarByClass(TikaIndexerTool.class);
    job2.setInputFormatClass(NLineInputFormat.class);
    NLineInputFormat.addInputPath(job2, fullInputList);
    NLineInputFormat.setNumLinesPerSplit(job2, numLinesPerSplit);          
    job2.setMapperClass(LineRandomizerMapper.class);
    job2.setReducerClass(LineRandomizerReducer.class);
    job2.setOutputFormatClass(TextOutputFormat.class);
    FileOutputFormat.setOutputPath(job2, outputStep2Dir);
    job2.setNumReduceTasks(1);
    job2.setOutputKeyClass(LongWritable.class);
    job2.setOutputValueClass(Text.class);
    return job2;
  }

  private long addInputFiles(List<Path> inputFiles, List<String> inputLists, Path fullInputList) throws IOException {
    long numFiles = 0;
    FileSystem fs = fullInputList.getFileSystem(job.getConfiguration());
    FSDataOutputStream out = fs.create(fullInputList);
    try {
      Writer writer = new OutputStreamWriter(out, "UTF-8");
      
      for (Path inputFile : inputFiles) {
        numFiles += addInputFilesRecursively(inputFile, writer);
      }

      for (String inputList : inputLists) {
        InputStream in;
        if (inputList.equals("-")) {
          in = System.in;
        } else {
          Path path = new Path(inputList);
          in = fs.open(path);
        }
        try {
          BufferedReader reader = new BufferedReader(new InputStreamReader(in, "UTF-8"));
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
  private long addInputFilesRecursively(Path path, Writer writer) throws IOException {
    long numFiles = 0;
    FileSystem fs = path.getFileSystem(job.getConfiguration());
    if (!fs.exists(path)) {
      return numFiles;
    }
    for (FileStatus stat : fs.listStatus(path)) {
      LOG.debug("Processing path {}", stat.getPath());
      if (stat.isDirectory()) {
        numFiles += addInputFilesRecursively(stat.getPath(), writer);
      } else {
        writer.write(stat.getPath().toString() + "\n");
        numFiles++;
      }
    }
    return numFiles;
  }
  
  private void checkHdfsPath(Path path) {
    if (!HdfsConstants.HDFS_URI_SCHEME.equals(path.toUri().getScheme())) {
      throw new IllegalArgumentException("Not an HDFS path: " + path);
    }
  }
  
}
