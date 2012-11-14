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


import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.solr.hadoop.SolrDocumentConverter;
import org.apache.solr.hadoop.SolrOutputFormat;
import org.apache.solr.hadoop.SolrReducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TikaIndexer extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(TikaIndexer.class);

  /** A list of input file URLs. Used as input to the Mapper */
  private static final String SOLR_NLIST_FILE = "solr_nlist_file.txt";

  private final Job job;
  private final List<FileStatus> inputFiles = new LinkedList<FileStatus>();

  private Path solrNlistFile;

  public TikaIndexer() throws IOException {
    job = Job.getInstance(getConf());
  }

  protected void usage() {
    System.err.println("Usage: TikaIndexer <outputDir> -solr <solrHome> -mappers NNN <inputDir> [<inputDir2> ...] [-shards NNN]");
    System.err.println("\tinputDir\tinput directory(-ies) containing files");
    System.err.println("\toutputDir\toutput directory containing Solr indexes.");
    System.err.println("\tsolr <solrHome>\tlocal directory containing Solr conf/ and lib/");
    System.err.println("\tmappers NNN\tmaximum number of mappers to use");
    System.err.println("\tshards NNN\tset the number of output shards to NNN");
    System.err.println("\t\t(default: the default number of reduce tasks)");
  }

  /**
   * Add the specified file to the input set, if p is a directory then
   * add the files contained therein.
   */
  protected void processInputDirectory(Path p) throws IOException {
    FileSystem fs = p.getFileSystem(job.getConfiguration());
    if (!fs.exists(p)) {
      return;
    }
    for (FileStatus stat : fs.listStatus(p)) {
      if (stat.isDirectory()) {
        processInputDirectory(stat.getPath());
      } else {
        inputFiles.add(stat);
      }
    }
  }

  /** Write the collected input files to HDFS - will be the input for the Mapper */
  protected void finalizeInputPaths(Path outputDirectory) throws IOException {
    FileSystem fs = outputDirectory.getFileSystem(job.getConfiguration());

    LOG.info("Creating mapper input file {}", solrNlistFile.toString());

    PrintStream out = new PrintStream(fs.create(solrNlistFile));
    try {
      for(FileStatus stat: inputFiles) {
        out.println(stat.getPath().toString());
      }
    } finally {
      out.close();
    }
    FileInputFormat.addInputPath(job, solrNlistFile);
  }

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      usage();
      return -1;
    }
    job.setJarByClass(TikaIndexer.class);

    int shards = 1;
    int mappers = 1;
    String solrHome = null;

    Path outputDirectory = new Path(args[0]);
    solrNlistFile = new Path(outputDirectory, SOLR_NLIST_FILE);

    for (int i = 1; i < args.length; i++) {
      if (args[i] == null) continue;
      if (args[i].equals("-shards")) {
        shards = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-solr")) {
        solrHome = args[++i];
      } else if (args[i].equals("-mappers")) {
        mappers = Integer.parseInt(args[++i]);
      } else {
        processInputDirectory(new Path(args[i]));
      }
    }
    if (inputFiles.size() == 0) {
      throw new IOException("No input files found");
    }
    finalizeInputPaths(outputDirectory);

    if (solrHome == null || !new File(solrHome).exists()) {
      throw new IOException("You must specify a valid solr.home directory!");
    }

    job.setInputFormatClass(NLineInputFormat.class);
    NLineInputFormat.setNumLinesPerSplit(job, inputFiles.size() / mappers);

    job.setMapperClass(TikaMapper.class);
    job.setReducerClass(SolrReducer.class);

    job.setOutputFormatClass(SolrOutputFormat.class);
    SolrOutputFormat.setupSolrHomeCache(new File(solrHome), job);

    job.setNumReduceTasks(shards);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(MapWritable.class);
    SolrDocumentConverter.setSolrDocumentConverter(TikaDocumentConverter.class, job.getConfiguration());
    FileOutputFormat.setOutputPath(job, outputDirectory);

    return job.waitForCompletion(true) ? 0 : -1;
  }
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    int res = ToolRunner.run(conf, new TikaIndexer(), args);
    System.exit(res);
  }

}
