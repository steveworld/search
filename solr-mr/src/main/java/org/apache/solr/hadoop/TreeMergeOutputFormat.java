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

package org.apache.solr.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.misc.IndexMergeTool;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.apache.solr.store.hdfs.HdfsDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * See {@link IndexMergeTool}.
 */
public class TreeMergeOutputFormat extends FileOutputFormat<Text, NullWritable> {
  
  @Override
  public RecordWriter getRecordWriter(TaskAttemptContext context) throws IOException {
    Path workDir = getDefaultWorkFile(context, "");
    return new TreeMergeRecordWriter(context, workDir);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class TreeMergeRecordWriter extends RecordWriter<Text,NullWritable> {
    
    private Path workDir;
    private List<Path> shards = new ArrayList();
    private HeartBeater heartBeater;
    
    private static final Logger LOG = LoggerFactory.getLogger(TreeMergeRecordWriter.class);

    public TreeMergeRecordWriter(TaskAttemptContext context, Path workDir) {
      this.workDir = new Path(workDir, "data/index");
      heartBeater = new HeartBeater(context);
    }
    
    @Override
    public void write(Text key, NullWritable value) {
      LOG.info("map key: {}", key);
      heartBeater.needHeartBeat();
      try {
        Path path = new Path(key.toString());
        shards.add(path);
      } finally {
        heartBeater.cancelHeartBeat();
      }
    }
    
    @Override
    public void close(TaskAttemptContext context) throws IOException {
      LOG.debug("Merging into dstDir: " + workDir + ", srcDirs: {}", shards);
      heartBeater.needHeartBeat();
      try {
        Directory mergedIndex = new HdfsDirectory(workDir, context.getConfiguration());
        
        IndexWriterConfig writerConfig = new IndexWriterConfig(Version.LUCENE_CURRENT, null)
            .setOpenMode(OpenMode.CREATE)
            //.setMergePolicy(mergePolicy) // TODO: grab tuned MergePolicy from solrconfig.xml?
            //.setMergeScheduler(...) // TODO: grab tuned MergeScheduler from solrconfig.xml?
            ;
        IndexWriter writer = new IndexWriter(mergedIndex, writerConfig);
        
        Directory[] indexes = new Directory[shards.size()];
        for (int i = 0; i < shards.size(); i++) {
          indexes[i] = new HdfsDirectory(shards.get(i), context.getConfiguration());
        }

        context.setStatus("Logically merging " + shards.size() + " shards into one shard");
        LOG.info("Logically merging " + shards.size() + " shards into one shard: " + workDir);
        long start = System.currentTimeMillis();
        
        writer.addIndexes(indexes); 
        // TODO: instead consider using addIndexes(IndexReader... readers) to avoid intermediate 
        // copying of files into dst directory before running the physical segment merge. 
        // This can improve performance and turns this phase into a true "logical" merge.
        
        float secs = (System.currentTimeMillis() - start) / 1000.0f;
        LOG.info("Logical merge took {} secs", secs);

        int maxSegments = context.getConfiguration().getInt(BatchWriter.MAX_SEGMENTS, 1);
        context.setStatus("Optimizing Solr: forcing mtree merge down to " + maxSegments + " segments");
        LOG.info("Optimizing Solr: forcing tree merge down to {} segments", maxSegments);
        start = System.currentTimeMillis();
        writer.forceMerge(maxSegments);
        writer.close();
        secs = (System.currentTimeMillis() - start) / 1000.0f;
        LOG.info("Optimizing Solr: done forcing tree merge down to {} segments in {} secs", maxSegments, secs);
        context.setStatus("Done");
      } finally {
        heartBeater.cancelHeartBeat();
      }
    }    
  }
}
