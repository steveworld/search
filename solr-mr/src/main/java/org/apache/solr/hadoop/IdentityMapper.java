package org.apache.solr.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IdentityMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(IdentityMapper.class);

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    LOGGER.info("map key: {}, value: {}", key, value);
    context.write(value, NullWritable.get());
  }
}