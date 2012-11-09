/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.sink.solr;

import java.lang.reflect.Array;

import org.apache.flume.instrumentation.MonitoredCounterGroup;

public class SimpleSolrSinkCounter extends MonitoredCounterGroup implements SimpleSolrSinkCounterMBean {

  /*
   * I wish org.apache.flume.instrumentation.SinkCounter had a public
   * constructor SinkCounter(Type type, String name, String... attrs) so we
   * could inherit from that rather than copy n' paste it all...
   */

  private static final String COUNTER_TAKE_MICROS = "solrsink.take.micros";

  private static final String COUNTER_EXTRACT_MICROS = "solrsink.extract.micros";

  private static final String COUNTER_TRANSFORM_MICROS = "solrsink.transform.micros";

  private static final String COUNTER_LOAD_MICROS = "solrsink.load.micros";

  private static final String[] ATTRIBUTES_2 = { COUNTER_TAKE_MICROS, COUNTER_EXTRACT_MICROS, COUNTER_TRANSFORM_MICROS,
      COUNTER_LOAD_MICROS };

  private static final String COUNTER_CONNECTION_CREATED = "sink.connection.creation.count";

  private static final String COUNTER_CONNECTION_CLOSED = "sink.connection.closed.count";

  private static final String COUNTER_CONNECTION_FAILED = "sink.connection.failed.count";

  private static final String COUNTER_BATCH_EMPTY = "sink.batch.empty";

  private static final String COUNTER_BATCH_UNDERFLOW = "sink.batch.underflow";

  private static final String COUNTER_BATCH_COMPLETE = "sink.batch.complete";

  private static final String COUNTER_EVENT_DRAIN_ATTEMPT = "sink.event.drain.attempt";

  private static final String COUNTER_EVENT_DRAIN_SUCCESS = "sink.event.drain.sucess";

  private static final String[] ATTRIBUTES = { COUNTER_CONNECTION_CREATED, COUNTER_CONNECTION_CLOSED,
      COUNTER_CONNECTION_FAILED, COUNTER_BATCH_EMPTY, COUNTER_BATCH_UNDERFLOW, COUNTER_BATCH_COMPLETE,
      COUNTER_EVENT_DRAIN_ATTEMPT, COUNTER_EVENT_DRAIN_SUCCESS };

  public SimpleSolrSinkCounter(String name) {
    super(MonitoredCounterGroup.Type.SINK, name, concat(ATTRIBUTES, ATTRIBUTES_2));
  }

  @Override
  public long getConnectionCreatedCount() {
    return get(COUNTER_CONNECTION_CREATED);
  }

  public long incrementConnectionCreatedCount() {
    return increment(COUNTER_CONNECTION_CREATED);
  }

  @Override
  public long getConnectionClosedCount() {
    return get(COUNTER_CONNECTION_CLOSED);
  }

  public long incrementConnectionClosedCount() {
    return increment(COUNTER_CONNECTION_CLOSED);
  }

  @Override
  public long getConnectionFailedCount() {
    return get(COUNTER_CONNECTION_FAILED);
  }

  public long incrementConnectionFailedCount() {
    return increment(COUNTER_CONNECTION_FAILED);
  }

  @Override
  public long getBatchEmptyCount() {
    return get(COUNTER_BATCH_EMPTY);
  }

  public long incrementBatchEmptyCount() {
    return increment(COUNTER_BATCH_EMPTY);
  }

  @Override
  public long getBatchUnderflowCount() {
    return get(COUNTER_BATCH_UNDERFLOW);
  }

  public long incrementBatchUnderflowCount() {
    return increment(COUNTER_BATCH_UNDERFLOW);
  }

  @Override
  public long getBatchCompleteCount() {
    return get(COUNTER_BATCH_COMPLETE);
  }

  public long incrementBatchCompleteCount() {
    return increment(COUNTER_BATCH_COMPLETE);
  }

  @Override
  public long getEventDrainAttemptCount() {
    return get(COUNTER_EVENT_DRAIN_ATTEMPT);
  }

  public long incrementEventDrainAttemptCount() {
    return increment(COUNTER_EVENT_DRAIN_ATTEMPT);
  }

  public long addToEventDrainAttemptCount(long delta) {
    return addAndGet(COUNTER_EVENT_DRAIN_ATTEMPT, delta);
  }

  @Override
  public long getEventDrainSuccessCount() {
    return get(COUNTER_EVENT_DRAIN_SUCCESS);
  }

  public long incrementEventDrainSuccessCount() {
    return increment(COUNTER_EVENT_DRAIN_SUCCESS);
  }

  public long addToEventDrainSuccessCount(long delta) {
    return addAndGet(COUNTER_EVENT_DRAIN_SUCCESS, delta);
  }

  @Override
  public long getTakeMillis() {
    return get(COUNTER_TAKE_MICROS) / 1000;
  }

  @Override
  public long getExtractMillis() {
    return get(COUNTER_EXTRACT_MICROS) / 1000;
  }

  @Override
  public long getTransformMillis() {
    return get(COUNTER_TRANSFORM_MICROS) / 1000;
  }

  @Override
  public long getLoadMillis() {
    return get(COUNTER_LOAD_MICROS) / 1000;
  }

  public long addToTakeNanos(long delta) {
    return addAndGet(COUNTER_TAKE_MICROS, delta / 1000);
  }

  public long addToExtractNanos(long delta) {
    return addAndGet(COUNTER_EXTRACT_MICROS, delta / 1000);
  }

  public long addToTransformNanos(long delta) {
    return addAndGet(COUNTER_TRANSFORM_MICROS, delta / 1000);
  }

  public long addToLoadNanos(long delta) {
    return addAndGet(COUNTER_LOAD_MICROS, delta / 1000);
  }

  private static <T> T[] concat(T[]... arrays) {
    if (arrays.length == 0) {
      throw new IllegalArgumentException();
    }
    Class clazz = null;
    int length = 0;
    for (T[] array : arrays) {
      clazz = array.getClass();
      length += array.length;
    }
    T[] result = (T[]) Array.newInstance(clazz.getComponentType(), length);
    int pos = 0;
    for (T[] array : arrays) {
      System.arraycopy(array, 0, result, pos, array.length);
      pos += array.length;
    }
    return result;
  }

}
