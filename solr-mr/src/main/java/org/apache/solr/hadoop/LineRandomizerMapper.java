package org.apache.solr.hadoop;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LineRandomizerMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

  private Random random;
  
  private static final Logger LOGGER = LoggerFactory.getLogger(LineRandomizerMapper.class);

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    long taskId = 0;
    if (context.getTaskAttemptID() != null) { // MRUnit returns null
      LOGGER.info("context.getTaskAttemptID().getId(): {}", context.getTaskAttemptID().getId());
      LOGGER.info("context.getTaskAttemptID().getTaskID().getId(): {}", context.getTaskAttemptID().getTaskID().getId());
      taskId = context.getTaskAttemptID().getTaskID().getId(); // taskId = 0, 1, ..., N
    }
    random = new Random(123456789012345678L + taskId); // deterministic PNRG sequence for easy reproducability
  }

  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    LOGGER.debug("map key: {}, value: {}", key, value);
    context.write(new LongWritable(random.nextLong()), value);
  }
}