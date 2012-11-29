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
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.solr.hadoop.SolrDocumentConverter;
import org.apache.solr.hadoop.SolrOutputFormat;
import org.apache.solr.hadoop.SolrReducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TikaIndexerTool extends Configured implements Tool {
  
  private static final Logger LOG = LoggerFactory.getLogger(TikaIndexerTool.class);

  /** A list of input file URLs. Used as input to the Mapper */
  private static final String SOLR_NLIST_FILE = "solr_nlist_file.txt";

  private Job job;

  private void usage() {
    String msg = 
        "Usage: hadoop jar solr-mr.jar " + getClass().getName() + " [options]... [inputFileOrDir]... " +
        "\n" + 
        "\nSpecific options supported are" +
        "\n\n-outputdir <hdfsOutputDir>\tHDFS output directory containing Solr indexes (defaults to hdfs:///user/" + System.getProperty("user.name") + "/tikaindexer-output)" +
        "\n\n-inputlist <File>\tLocal or HDFS file containing a list of HDFS URLs, one URL per line. If '-' is specified, URLs are read from the standard input." +
        "\n\n-solr <solrHome>\tLocal dir containing Solr conf/ and lib/ (defaults to " + System.getProperty("user.home") + File.separator + "solr)" +
        "\n\n-mappers <NNN>\tMaximum number of mappers to use (defaults to 1)" +
        "\n\n-shards NNN\tNumber of output shards (defaults to 1)" +
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

    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-outputdir")) {
        outputDir = new Path(URI.create(args[0]));
        checkHdfsPath(outputDir);
      } else if (args[i].equals("-inputlist")) {
        inputLists.add(args[++i]);
      } else if (args[i].equals("-solr")) {
        solrHomeDir = new File(args[++i]);
      } else if (args[i].equals("-shards")) {
        shards = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-mappers")) {
        mappers = Integer.parseInt(args[++i]);
      } else {
        Path path = new Path(URI.create(args[i]));
        checkHdfsPath(path);
        inputFiles.add(path);
      }
    }
    
    if (!solrHomeDir.exists()) {
      throw new IOException("Solr home directory does not exist: " + solrHomeDir);
    }

    Path solrNlistFile = new Path(outputDir, SOLR_NLIST_FILE);
    long numFiles = addInputFiles(outputDir, solrNlistFile, inputLists, inputFiles);
    if (numFiles == 0) {
      throw new IOException("No input files found");
    }

    // TODO: randomize solrNlistFile with separate little Mapper & Reducer for preprocessing
    
    job.setInputFormatClass(NLineInputFormat.class);
    NLineInputFormat.addInputPath(job, solrNlistFile);
    int numLinesPerSplit = (int) (numFiles / mappers);
    if (numLinesPerSplit < 0) { // numeric overflow from downcasting long to int?
      numLinesPerSplit = Integer.MAX_VALUE;
    }
    numLinesPerSplit = Math.max(1, numLinesPerSplit);
    NLineInputFormat.setNumLinesPerSplit(job, numLinesPerSplit);    

    job.setMapperClass(TikaMapper.class);
    job.setReducerClass(SolrReducer.class);

    job.setOutputFormatClass(SolrOutputFormat.class);
    SolrOutputFormat.setupSolrHomeCache(solrHomeDir, job);

    job.setNumReduceTasks(shards);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(MapWritable.class);
    SolrDocumentConverter.setSolrDocumentConverter(TikaDocumentConverter.class, job.getConfiguration());
    FileOutputFormat.setOutputPath(job, outputDir);

    return job.waitForCompletion(true) ? 0 : -1;
  }

  private long addInputFiles(Path outputDir, Path solrNlistFile, List<String> inputLists, List<Path> inputFiles) throws IOException {
    LOG.info("Creating mapper input file {}", solrNlistFile);
    long numFiles = 0;
    FileSystem fs = outputDir.getFileSystem(job.getConfiguration());
    FSDataOutputStream out = fs.create(solrNlistFile);
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
          Path path = new Path(URI.create(inputList));
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
