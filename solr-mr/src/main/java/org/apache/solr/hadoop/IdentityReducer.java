package org.apache.solr.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IdentityReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(IdentityReducer.class);

  @Override
  protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
    LOGGER.info("reduce key: {}, value: {}", key, values);
    context.write(key, NullWritable.get());
  }
}