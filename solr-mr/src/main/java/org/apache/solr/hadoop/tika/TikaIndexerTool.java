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


import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.impl.action.HelpArgumentAction;
import net.sourceforge.argparse4j.impl.choice.RangeArgumentChoice;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
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
import org.apache.solr.hadoop.SolrInputDocumentWritable;
import org.apache.solr.hadoop.SolrOutputFormat;
import org.apache.solr.hadoop.SolrReducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** 
 * Command line tool that runs an MR job that creates a Solr index from a set of input files.
 */
public class TikaIndexerTool extends Configured implements Tool {
  
  public static final String RESULTS_DIR = "results";

  /** A list of input file URLs. Used as input to the Mapper */
  private static final String FULL_INPUT_LIST = "full-input-list.txt";

  private static final Logger LOG = LoggerFactory.getLogger(TikaIndexerTool.class);

  
  public static void main(String[] args) throws Exception  {
    Configuration conf = new Configuration();
    int res = ToolRunner.run(conf, new TikaIndexerTool(), args);
    System.exit(res);
  }

  private Namespace parseArgs(String[] args, FileSystem fs) {
    final int argParserErrorExitCode = 1;
    ArgumentParser parser = ArgumentParsers
        .newArgumentParser("hadoop [GenericOptions]... jar solr-mr-*-job.jar " //+ getClass().getName()
            , false)
        .defaultHelp(true)
        .description(
            "Map Reduce job that creates a Solr index from a set of input files "
                + "and puts the data into HDFS, in a scalable and fault-tolerant manner.");

    parser.addArgument("--help", "-h")
      .help("show this help message and exit")
      .action(new HelpArgumentAction() {
        @Override
        public void run(ArgumentParser parser, Argument arg, Map<String, Object> attrs, String flag, Object value) throws ArgumentParserException {
          parser.printHelp(new PrintWriter(System.out));  
          System.out.println();
          ToolRunner.printGenericCommandUsage(System.out);
          System.out.println(
            "Examples: \n\n" + 
            "sudo -u hdfs hadoop --config /etc/hadoop/conf.cloudera.mapreduce1" +
            " jar solr-mr-*-job.jar " + //TikaIndexerTool.class.getName() +
            " --solrhomedir /home/foo/solr" +
            " --outputdir hdfs:///user/foo/tikaindexer-output" + 
            " hdfs:///user/foo/tikaindexer-input"  
            );
          System.exit(0);
        }        
      });
    
    parser.addArgument("--inputlist")
      .action(Arguments.append())
      .metavar("URI")
//      .type(new ArgumentTypes.PathArgumentType(fs).verifyExists().verifyCanRead())
      .type(Path.class)
      .help("Local URI or HDFS URI of a file containing a list of HDFS URIs to index, one URI per line. " + 
            "If '-' is specified, URIs are read from the standard input. " + 
            "Multiple --inputlist arguments can be specified");
    
    parser.addArgument("--outputdir")
      .metavar("HDFS_URI")
      .type(new ArgumentTypes.PathArgumentType(fs).verifyScheme(fs.getScheme()).verifyCanWriteParent())
      .required(true)
      .help("HDFS directory to write Solr indexes to");
    
    parser.addArgument("--solrhomedir")
      .metavar("DIR")
      .type(new ArgumentTypes.FileArgumentType().verifyIsDirectory().verifyCanRead())
      .required(true)
      .help("Local dir containing Solr conf/ and lib/");

    parser.addArgument("--mappers")
      .metavar("INTEGER")
      .type(Integer.class)
      .choices(new RangeArgumentChoice(-1, Integer.MAX_VALUE))
      .setDefault(1)
      .help("Maximum number of MR mappers to use");

    parser.addArgument("--shards")
      .metavar("INTEGER")
      .type(Integer.class)
      .choices(new RangeArgumentChoice(1, Integer.MAX_VALUE))
      .setDefault(1)
      .help("Number of output shards to use");

    parser.addArgument("--fairschedulerpool")
      .metavar("STRING")
      .help("Name of MR fair scheduler pool to submit job to");

    parser.addArgument("--verbose", "-v")
      .action(Arguments.storeTrue())
      .help("Turn on verbose output");

    parser.addArgument("--norandomize")
      .action(Arguments.storeTrue())
      .help("undocumented and subject to removal without notice");

    parser.addArgument("--identitytest")
      .action(Arguments.storeTrue())
      .help("undocumented and subject to removal without notice");

    // trailing positional arguments
    parser.addArgument("inputfiles")
      .metavar("HDFS_URI")
      .type(new ArgumentTypes.PathArgumentType(fs).verifyScheme(fs.getScheme()).verifyExists().verifyCanRead())
      .nargs("*")
      .setDefault()
      .help("HDFS URI of file or dir to index");

    Namespace ns = null;
    try {
      ns = parser.parseArgs(args);
    } catch (ArgumentParserException e) {
      parser.handleError(e);
      System.exit(argParserErrorExitCode);
    }
    
    LOG.debug("Command line args: {}", ns);
    return ns;
  }

  @Override
  public int run(String[] args) throws Exception {
    FileSystem fs = FileSystem.get(getConf());

    Namespace ns = parseArgs(args, fs);
    
    List<Path> inputLists = ns.getList("inputlist");
    if (inputLists == null) {
      inputLists = Collections.EMPTY_LIST;
    }
    List<Path> inputFiles = ns.getList("inputfiles");
    if (inputLists.isEmpty() && inputFiles.isEmpty()) {
      return 0; // nothing to do
    }
    Path outputDir = (Path) ns.get("outputdir");
    int mappers = ns.getInt("mappers");
    int shards = ns.getInt("shards");
    File solrHomeDir = (File) ns.get("solrhomedir");
    String fairSchedulerPool = (String) ns.get("fairschedulerpool");
    boolean isRandomize = !ns.getBoolean("norandomize");
    boolean isVerbose = ns.getBoolean("verbose");
    boolean isIdentityTest = ns.getBoolean("identitytest");

    Job job = Job.getInstance(getConf());
    job.setJarByClass(getClass());
    job.setJobName(getClass().getName());
    
    if (mappers == -1) { 
      mappers = new JobClient(job.getConfiguration()).getClusterStatus().getMaxMapTasks(); // MR1
      //mappers = job.getCluster().getClusterStatus().getMapSlotCapacity(); // Yarn
      LOG.info("Choosing dynamic number of mappers: {}", mappers);
    }
    if (mappers <= 0) {
      throw new IllegalStateException("Illegal number of mappers: " + mappers);
    }
    
    fs.delete(outputDir, true);    
    Path outputResultsDir = new Path(outputDir, RESULTS_DIR);    
    Path outputStep1Dir = new Path(outputDir, "tmp1");    
    Path outputStep2Dir = new Path(outputDir, "tmp2");    
    Path fullInputList = new Path(outputStep1Dir, FULL_INPUT_LIST);
    
    LOG.info("Creating full input list file for solr mappers {}", fullInputList);
    long numFiles = addInputFiles(inputFiles, inputLists, fullInputList, job.getConfiguration());
    if (numFiles == 0) {
      throw new IOException("No input files found");
    }
    int numLinesPerSplit = (int) (numFiles / mappers);
    if (numLinesPerSplit < 0) { // numeric overflow from downcasting long to int?
      numLinesPerSplit = Integer.MAX_VALUE;
    }
    numLinesPerSplit = Math.max(1, numLinesPerSplit);
//    numLinesPerSplit = Math.min(100000, numLinesPerSplit);

    if (isRandomize) { 
      Job randomizerJob = randomizeInputFiles(fullInputList, outputStep2Dir, numLinesPerSplit, fairSchedulerPool);
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
    if (fairSchedulerPool != null) {
      job.getConfiguration().set("mapred.fairscheduler.pool", fairSchedulerPool);
    }

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
      job.setOutputValueClass(SolrInputDocumentWritable.class);
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
   * might be randomized into the following output list of URLs:
   * 
   * C
   * A
   * D
   * B
   */
  private Job randomizeInputFiles(Path fullInputList, Path outputStep2Dir, int numLinesPerSplit, String fairSchedulerPool) throws IOException {
    LOG.info("Randomizing full input list file for solr mappers {}", fullInputList);
    Job job2 = Job.getInstance(new Configuration(getConf()));
    job2.setJarByClass(getClass());
    job2.setJobName(getClass().getName() + "-randomizer");
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
    if (fairSchedulerPool != null) {
      job2.getConfiguration().set("mapred.fairscheduler.pool", fairSchedulerPool);
    }
    return job2;
  }

  private long addInputFiles(List<Path> inputFiles, List<Path> inputLists, Path fullInputList, Configuration conf) throws IOException {
    long numFiles = 0;
    FileSystem fs = fullInputList.getFileSystem(conf);
    FSDataOutputStream out = fs.create(fullInputList);
    try {
      Writer writer = new OutputStreamWriter(out, "UTF-8");
      
      for (Path inputFile : inputFiles) {
        numFiles += addInputFilesRecursively(inputFile, writer, conf);
      }

      for (Path inputList : inputLists) {
        InputStream in;
        if (inputList.toString().equals("-")) {
          in = System.in;
        } else if (inputList.isAbsoluteAndSchemeAuthorityNull()) {
          in = new BufferedInputStream(new FileInputStream(inputList.toString()));
        } else {
          in = fs.open(inputList);
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
  private long addInputFilesRecursively(Path path, Writer writer, Configuration conf) throws IOException {
    long numFiles = 0;
    FileSystem fs = path.getFileSystem(conf);
    if (!fs.exists(path)) {
      return numFiles;
    }
    for (FileStatus stat : fs.listStatus(path)) {
      LOG.debug("Processing path {}", stat.getPath());
      if (stat.isDirectory()) {
        numFiles += addInputFilesRecursively(stat.getPath(), writer, conf);
      } else {
        writer.write(stat.getPath().toString() + "\n");
        numFiles++;
      }
    }
    return numFiles;
  }
    
}
