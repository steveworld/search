/**
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.cdk.morphline.api;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.cloudera.cdk.morphline.base.Connector;
import com.cloudera.cdk.morphline.base.MorphlineBuilder;
import com.typesafe.config.Config;
import com.yammer.metrics.core.MetricsRegistry;

public class MorphlineTest extends Assert {
  
  private Collector collector;
  private Command morphline;
  
//  protected static final String RESOURCES_DIR = "target/test-classes";
  
  @Before
  public void setUp() throws Exception {
    collector = new Collector();
  }
  
  @After
  public void tearDown() throws Exception {
    collector = null;
  }
    
  @Test
  public void testComplexParse() throws Exception {
    parse("test-morphlines/testComplexParse-morphline");
  }
  
  @Test
  public void testBasic() throws Exception {
    Config config = parse("test-morphlines/testBasic-morphline");    
    morphline = createMorphline(config);    
    Record record = createBasicRecord();
    morphline.startSession();
    morphline.process(record);
    assertEquals(Arrays.asList(record), collector.getRecords());
    assertEquals(1, collector.getNumStartEvents());
  }

  @Test
  public void testBasicFilterPass() throws Exception {
    Config config = parse("test-morphlines/testBasicFilterPass-morphline");    
    morphline = createMorphline(config);
    Record record = new Record();
    record.getFields().put("first_name", "Nadja");
    List<Record> expectedList = new ArrayList();
    for (int i = 0; i < 2; i++) {
      Record expected = record.copy();
      expected.getFields().put("foo", "bar");
      expected.getFields().replaceValues("iter", Arrays.asList(i));
      expectedList.add(expected);
    }
    morphline.startSession();
    morphline.process(record);
    assertEquals(expectedList, collector.getRecords());
    assertTrue(record != collector.getRecords().get(0));
    assertEquals(1, collector.getNumStartEvents());
  }

  @Test
  public void testBasicFilterFail() throws Exception {
    Config config = parse("test-morphlines/testBasicFilterFail-morphline");    
    morphline = createMorphline(config);
    Record record = new Record();
    record.getFields().put("first_name", "Nadja");
    List<Record> expectedList = new ArrayList();
    for (int i = 0; i < 2; i++) {
      Record expected = record.copy();
      expected.getFields().put("foo2", "bar2");
      expected.getFields().replaceValues("iter2", Arrays.asList(i));
      expectedList.add(expected);
    }
    morphline.startSession();
    morphline.process(record);
    assertEquals(expectedList, collector.getRecords());
    assertTrue(record != collector.getRecords().get(0));
    assertEquals(1, collector.getNumStartEvents());
  }
  
  @Test
  public void testBasicFilterFailTwice() throws Exception {
    Config config = parse("test-morphlines/testBasicFilterFailTwice-morphline");    
    morphline = createMorphline(config);
    Record record = new Record();
    record.getFields().put("first_name", "Nadja");
    List<Record> expectedList = new ArrayList();
//    for (int i = 0; i < 2; i++) {
//      Record expected = new Record(record);
//      expected.getFields().put("foo2", "bar2");
//      expected.getFields().replaceValues("iter2", Arrays.asList(i));
//      expectedList.add(expected);
//    }
    morphline.startSession();
    try {
      morphline.process(record);
      fail();
    } catch (MorphlineRuntimeException e) {
      assertTrue(e.getMessage().startsWith("Filter found no matching rule"));
    }
    assertEquals(expectedList, collector.getRecords());
    assertEquals(1, collector.getNumStartEvents());
  }
  
  @Test
  public void testIfThenElseBasicThen() throws Exception {
    Config config = parse("test-morphlines/testIfThenElseBasicThen-morphline");    
    morphline = createMorphline(config);
    Record record = createBasicRecord();
    morphline.startSession();
    morphline.process(record);
    assertEquals(Arrays.asList(record), collector.getRecords());
    assertEquals(1, collector.getNumStartEvents());
    assertEquals("then1", collector.getRecords().get(0).getFirstValue("state"));
  }
  
  @Test
  public void testIfThenElseBasicThenEmpty() throws Exception {
    Config config = parse("test-morphlines/testIfThenElseBasicThenEmpty-morphline");    
    morphline = createMorphline(config);
    Record record = createBasicRecord();
    morphline.startSession();
    morphline.process(record);
    assertEquals(Arrays.asList(record), collector.getRecords());
    assertEquals(1, collector.getNumStartEvents());
    assertEquals("init1", collector.getRecords().get(0).getFirstValue("state"));
  }
  
  @Test
  public void testIfThenElseBasicElse() throws Exception {
    Config config = parse("test-morphlines/testIfThenElseBasicElse-morphline");    
    morphline = createMorphline(config);
    Record record = createBasicRecord();
    morphline.startSession();
    morphline.process(record);
    assertEquals(Arrays.asList(record), collector.getRecords());
    assertEquals(1, collector.getNumStartEvents());
    assertEquals("else1", collector.getRecords().get(0).getFirstValue("state"));
  }
  
  @Test
  public void testIfThenElseBasicElseEmpty() throws Exception {
    Config config = parse("test-morphlines/testIfThenElseBasicElseEmpty-morphline");    
    morphline = createMorphline(config);
    Record record = createBasicRecord();
    morphline.startSession();
    morphline.process(record);
    assertEquals(Arrays.asList(record), collector.getRecords());
    assertEquals(1, collector.getNumStartEvents());
    assertEquals("init1", collector.getRecords().get(0).getFirstValue("state"));
  }
  
  @Test
  public void testNotBasicTrue() throws Exception {
    Config config = parse("test-morphlines/testNotBasicTrue-morphline");    
    morphline = createMorphline(config);
    Record record = createBasicRecord();
    morphline.startSession();
    assertFalse(morphline.process(record));
    assertEquals(Arrays.asList(record), collector.getRecords());
    assertEquals(1, collector.getNumStartEvents());
    assertEquals("touched", collector.getRecords().get(0).getFirstValue("state"));
  }
  
  @Test
  public void testNotBasicFalse() throws Exception {
    Config config = parse("test-morphlines/testNotBasicFalse-morphline");    
    morphline = createMorphline(config);
    Record record = createBasicRecord();
    morphline.startSession();
    assertTrue(morphline.process(record));
    assertEquals(Arrays.asList(), collector.getRecords());
    assertEquals(1, collector.getNumStartEvents());
  }
  
  @Test
  @Ignore
  public void testReflection() {
    long start = System.currentTimeMillis();
    List<String> packagePrefixes = Arrays.asList("com", "org", "net");
    for (Class clazz : new MorphlineContext(new MetricsRegistry()).getTopLevelClassesRecursive(
        packagePrefixes, CommandBuilder.class)) {
      System.out.println("found " + clazz);
    }
//    for (Class cmd : new Reflections("com", "org").getSubTypesOf(CommandBuilder.class)) {
//      System.out.println(cmd);
//    }
    float secs = (System.currentTimeMillis() - start) / 1000.0f;
    System.out.println("secs=" + secs);
  }
  
  private Command createMorphline(Config config) {
    return new MorphlineBuilder().build(config, new Connector(), collector, createMorphlineContext());
//  return new Morphline(config, new Connector(), collector, new MorphlineContext());
  }

  private MorphlineContext createMorphlineContext() {
    return new MorphlineContext(new MetricsRegistry());
  }
  
  private Record createBasicRecord() {
    Record record = new Record();
    record.getFields().put("first_name", "Nadja");
    record.getFields().put("age", 8);
    record.getFields().put("tags", "one");
    record.getFields().put("tags", 2);
    record.getFields().put("tags", "three");
    return record;
  }

  private Config parse(String file) {
    Config config = Configs.parse(file);
    config = config.getConfigList("morphlines").get(0);
    return config;
  }
  
}
