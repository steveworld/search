package org.apache.solr.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LineRandomizerReducer extends Reducer<LongWritable, Text, Text, NullWritable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(LineRandomizerReducer.class);

  @Override
  protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    for (Text value : values) {
      LOGGER.debug("reduce key: {}, value: {}", key, value);
      context.write(value, NullWritable.get());
    }
  }
}