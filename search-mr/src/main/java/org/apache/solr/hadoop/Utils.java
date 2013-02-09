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
package org.apache.solr.hadoop;

import java.util.Random;

import org.apache.hadoop.mapreduce.Mapper.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Utils {

  private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

  public static Random createRandom(Context context) {
    long taskId = 0;
    if (context.getTaskAttemptID() != null) { // MRUnit returns null
      LOGGER.debug("context.getTaskAttemptID().getId(): {}", context.getTaskAttemptID().getId());
      LOGGER.debug("context.getTaskAttemptID().getTaskID().getId(): {}", context.getTaskAttemptID().getTaskID().getId());
      taskId = context.getTaskAttemptID().getTaskID().getId(); // taskId = 0, 1, ..., N
    }
    // create a good random seed, yet ensure deterministic PRNG sequence for easy reproducability
    return new Random(421439783L * (taskId + 1));
  }

  public static String getShortClassName(Class clazz) {
    int i = clazz.getName().lastIndexOf('.'); // regular class
    int j = clazz.getName().lastIndexOf('$'); // inner class
    return clazz.getName().substring(1 + Math.max(i, j));
  }
  
}
