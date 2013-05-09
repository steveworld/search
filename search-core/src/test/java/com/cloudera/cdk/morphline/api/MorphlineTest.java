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

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;

import com.cloudera.cdk.morphline.base.Fields;
import com.cloudera.cdk.morphline.shaded.com.google.code.regexp.Matcher;
import com.cloudera.cdk.morphline.shaded.com.google.code.regexp.Pattern;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.io.Files;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;

public class MorphlineTest extends AbstractMorphlineTest {
  
  @Test
  public void testParseComplexConfig() throws Exception {
    parse("test-morphlines/parseComplexConfig");
    parse("test-morphlines/tutorialReadAvroContainer");
    parse("test-morphlines/tutorialReadJsonTestTweets");
  }
  
  @Test
  public void testParseVariables() throws Exception {
    System.setProperty("ENV_ZK_HOST", "zk.foo.com:2181/solr");
    System.setProperty("ENV_SOLR_URL", "http://foo.com:8983/solr/myCollection");
    System.setProperty("ENV_SOLR_LOCATOR", "{ collection : collection1 }");
    try {
      Config override = ConfigFactory.parseString("SOLR_LOCATOR : { collection : fallback } ");
      Config config = parse("test-morphlines/parseVariables", override);
      //System.out.println(config.root().render());
    } finally {
      System.clearProperty("ENV_ZK_HOST");
      System.clearProperty("ENV_SOLR_URL");  
      System.clearProperty("ENV_SOLR_LOCATOR");
    }
  }
  
  @Test
  public void testPipeWithTwoBasicCommands() throws Exception {
    morphline = createMorphline("test-morphlines/pipeWithTwoBasicCommands");    
    Record record = createBasicRecord();
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));
    assertEquals(Arrays.asList(record), collector.getRecords());
    assertSame(record, collector.getRecords().get(0));
  }

  @Test
  public void testAddValues() throws Exception {
    morphline = createMorphline("test-morphlines/addValues");    
    Record record = new Record();
    record.put("first_name", "Nadja");
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));
    Record expected = new Record();
    expected.put("first_name", "Nadja");
    expected.put("source_type", "text/log");
    expected.put("source_type", "text/log2");
    expected.put("source_host", 123);
    expected.put("name", "Nadja");
    expected.put("names", "Nadja");
    expected.put("pids", 456);
    expected.put("pids", "hello");
    assertEquals(Arrays.asList(expected), collector.getRecords());
    assertSame(record, collector.getRecords().get(0));
  }

  @Test
  public void testSetValues() throws Exception {
    morphline = createMorphline("test-morphlines/setValues");    
    Record record = new Record();
    record.put("first_name", "Nadja");
    record.put("source_type", "XXXX");
    record.put("source_type", "XXXX");
    record.put("source_host", 999);
    record.put("name", "XXXX");
    record.put("names", "XXXX");
    record.put("pids", 789);
    record.put("pids", "YYYY");

    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));
    Record expected = new Record();
    expected.put("first_name", "Nadja");
    expected.put("source_type", "text/log");
    expected.put("source_type", "text/log2");
    expected.put("source_host", 123);
    expected.put("name", "Nadja");
    expected.put("names", "Nadja");
    expected.put("pids", 456);
    expected.put("pids", "hello");
    assertEquals(Arrays.asList(expected), collector.getRecords());
    assertSame(record, collector.getRecords().get(0));
  }

  @Test
  public void testTryRulesPass() throws Exception {
    morphline = createMorphline("test-morphlines/tryRulesPass");    
    Record record = new Record();
    record.put("first_name", "Nadja");
    List<Record> expectedList = new ArrayList();
    for (int i = 0; i < 2; i++) {
      Record expected = record.copy();
      expected.put("foo", "bar");
      expected.replaceValues("iter", i);
      expectedList.add(expected);
    }
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));
    assertEquals(expectedList, collector.getRecords());
    assertNotSame(record, collector.getRecords().get(0));
  }

  @Test
  public void testTryRulesFail() throws Exception {
    morphline = createMorphline("test-morphlines/tryRulesFail");    
    Record record = new Record();
    record.put("first_name", "Nadja");
    List<Record> expectedList = new ArrayList();
    for (int i = 0; i < 2; i++) {
      Record expected = record.copy();
      expected.put("foo2", "bar2");
      expected.replaceValues("iter2", i);
      expectedList.add(expected);
    }
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));
    assertEquals(expectedList, collector.getRecords());
    assertNotSame(record, collector.getRecords().get(0));
  }
  
  @Test
  public void testTryRulesFailTwice() throws Exception {
    morphline = createMorphline("test-morphlines/tryRulesFailTwice");    
    Record record = new Record();
    record.put("first_name", "Nadja");
    List<Record> expectedList = new ArrayList();
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    try {
      morphline.process(record);
      fail();
    } catch (MorphlineRuntimeException e) {
      assertTrue(e.getMessage().startsWith("tryRules command found no successful rule for record"));
    }
    assertEquals(expectedList, collector.getRecords());
  }
  
  @Test
  public void testIsTrue() throws Exception {
    System.setProperty("MY_VARIABLE", "true");
    morphline = createMorphline("test-morphlines/isTrue");    
    Record record = createBasicRecord();
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));
    assertSame(record, collector.getFirstRecord());
    
    collector.reset();
    System.setProperty("MY_VARIABLE", "false");
    morphline = createMorphline("test-morphlines/isTrue");    
    assertFalse(morphline.process(createBasicRecord()));
    assertEquals(0, collector.getRecords().size());
    
    collector.reset();
    System.clearProperty("MY_VARIABLE");
    try {
      morphline = createMorphline("test-morphlines/isTrue");
      fail();
    } catch (ConfigException.UnresolvedSubstitution e) {
      ; 
    }
  }
  
  @Test
  public void testIfThenElseWithThen() throws Exception {
    morphline = createMorphline("test-morphlines/ifThenElseWithThen");    
    Record record = createBasicRecord();
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));
    assertEquals(Arrays.asList(record), collector.getRecords());
    assertSame(record, collector.getRecords().get(0));
    assertEquals("then1", collector.getRecords().get(0).getFirstValue("state"));
  }
  
  @Test
  public void testIfThenElseWithThenEmpty() throws Exception {
    morphline = createMorphline("test-morphlines/ifThenElseWithThenEmpty");    
    Record record = createBasicRecord();
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));
    assertEquals(Arrays.asList(record), collector.getRecords());
    assertSame(record, collector.getRecords().get(0));
    assertEquals("init1", collector.getRecords().get(0).getFirstValue("state"));
  }
  
  @Test
  public void testIfThenElseWithElse() throws Exception {
    morphline = createMorphline("test-morphlines/ifThenElseWithElse");    
    Record record = createBasicRecord();
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));
    assertEquals(Arrays.asList(record), collector.getRecords());
    assertSame(record, collector.getRecords().get(0));
    assertEquals("else1", collector.getRecords().get(0).getFirstValue("state"));
  }
  
  @Test
  public void testIfThenElseWithElseEmpty() throws Exception {
    morphline = createMorphline("test-morphlines/ifThenElseWithElseEmpty");    
    Record record = createBasicRecord();
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));
    assertEquals(Arrays.asList(record), collector.getRecords());
    assertSame(record, collector.getRecords().get(0));
    assertEquals("init1", collector.getRecords().get(0).getFirstValue("state"));
  }
  
  @Test
  public void testNotWithTrue() throws Exception {
    morphline = createMorphline("test-morphlines/notWithTrue");    
    Record record = createBasicRecord();
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertFalse(morphline.process(record));
    assertEquals(Arrays.asList(record), collector.getRecords());
    assertSame(record, collector.getRecords().get(0));
    assertEquals("touched", collector.getRecords().get(0).getFirstValue("state"));
  }
  
  @Test
  public void testNotWithFalse() throws Exception {
    morphline = createMorphline("test-morphlines/notWithFalse");    
    Record record = createBasicRecord();
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));
    assertEquals(Arrays.asList(), collector.getRecords());
  }
  
  @Test
  public void testReadClob() throws Exception {
    morphline = createMorphline("test-morphlines/readClob");    
    Record record = new Record();
    String msg = "foo";
    record.put(Fields.ATTACHMENT_BODY, msg.getBytes("UTF-8"));
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));
    Record expected = new Record();
    expected.put(Fields.MESSAGE, msg);
    assertEquals(Arrays.asList(expected), collector.getRecords());
    assertNotSame(record, collector.getRecords().get(0));
  }
  
  @Test
  public void testReadCSV() throws Exception {
    morphline = createMorphline("test-morphlines/readCSV");    
    InputStream in = new FileInputStream(new File(RESOURCES_DIR + "/test-documents/cars.csv"));
    Record record = new Record();
    record.put(Fields.ATTACHMENT_BODY, in);
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));
    
    ImmutableMultimap expected;
    Iterator<Record> iter = collector.getRecords().iterator();
    expected = ImmutableMultimap.of("Year", "Year", "Description", "Description", "Model", "Model", "column4", "Price");
    assertEquals(expected, iter.next().getFields());
    expected = ImmutableMultimap.of("Year", "1997", "Description", "ac, abs, moon", "Model", "E350", "column4", "3000.00");
    assertEquals(expected, iter.next().getFields());
    expected = ImmutableMultimap.of("Year", "1999", "Description", "", "Model", "Venture \"Extended Edition\"", "column4", "4900.00");
    assertEquals(expected, iter.next().getFields());
    expected = ImmutableMultimap.of("Year", "1999", "Description", "", "Model", "Venture \"Extended Edition, Very Large\"", "column4", "5000.00");
    assertEquals(expected, iter.next().getFields());
    expected = ImmutableMultimap.of("Year", "1996", "Description", "MUST SELL!\nair, moon roof, loaded", "Model", "Grand Cherokee", "column4", "4799.00");
    assertEquals(expected, iter.next().getFields());
    assertFalse(iter.hasNext());
    in.close();
  }  
  
  @Test
  public void testReadLine() throws Exception {
    morphline = createMorphline("test-morphlines/readCSV");
    String threeLines = "first\nsecond\nthird";
    byte[] in = threeLines.getBytes("UTF-8");
    morphline = createMorphline("test-morphlines/readLine"); // uses ignoreFirstLine : true
    Record record = new Record();
    record.put(Fields.ATTACHMENT_BODY, in);
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));
    
    Iterator<Record> iter = collector.getRecords().iterator();
    assertEquals(ImmutableMultimap.of(Fields.MESSAGE, "second"), iter.next().getFields());
    assertEquals(ImmutableMultimap.of(Fields.MESSAGE, "third"), iter.next().getFields());
    assertFalse(iter.hasNext());
  }  
  
  @Test
  public void testReadMultiLine() throws Exception {
    morphline = createMorphline("test-morphlines/readMultiLine");   
    InputStream in = new FileInputStream(new File(RESOURCES_DIR + "/test-documents/multiline-stacktrace.log"));
    Record record = new Record();
    record.put(Fields.ATTACHMENT_BODY, in);
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));
    
    Iterator<Record> iter = collector.getRecords().iterator();
    assertEquals(ImmutableMultimap.of(Fields.MESSAGE, "juil. 25, 2012 10:49:46 AM hudson.triggers.SafeTimerTask run"), iter.next().getFields());
    String multiLineEvent = Files.toString(new File(RESOURCES_DIR + "/test-documents/multiline-stacktrace-expected-long-event.log"), Charsets.UTF_8);
    assertEquals(ImmutableMultimap.of(Fields.MESSAGE, multiLineEvent), iter.next().getFields());
    assertEquals(ImmutableMultimap.of(Fields.MESSAGE, "juil. 25, 2012 10:49:54 AM hudson.slaves.SlaveComputer tryReconnect"), iter.next().getFields());
    assertEquals(ImmutableMultimap.of(Fields.MESSAGE, "Infos: Attempting to reconnect CentosVagrant"), iter.next().getFields());
    assertFalse(iter.hasNext());    
    in.close();
  }  

  @Test
  public void testJavaHelloWorld() throws Exception {
    morphline = createMorphline("test-morphlines/javaHelloWorld");    
    Record record = new Record();
    record.put("tags", "hello");
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));
    Record expected = new Record();
    expected.put("tags", "hello");
    expected.put("tags", "world");
    assertEquals(Arrays.asList(expected), collector.getRecords());
    assertSame(record, collector.getRecords().get(0));
  }
  
  @Test
  public void testJavaRuntimeException() throws Exception {
    morphline = createMorphline("test-morphlines/javaRuntimeException");    
    Record record = new Record();
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    try {
      morphline.process(record);
      fail();
    } catch (MorphlineRuntimeException e) {
      assertTrue(e.getMessage().startsWith("Cannot execute script"));
    }
    assertEquals(Arrays.asList(), collector.getRecords());
  }
  
  @Test
  public void testJavaCompilationException() throws Exception {
    Config config = parse("test-morphlines/javaCompilationException");    
    try {
      createMorphline(config);
      fail();
    } catch (MorphlineCompilationException e) {
      assertTrue(e.getMessage().startsWith("Cannot compile script"));
    }
  }
  
  @Test
  public void testGrokSyslogMatch() throws Exception {
    testGrokSyslogMatchInternal(false, false);
  }
  
  @Test
  public void testGrokSyslogMatchInplace() throws Exception {
    testGrokSyslogMatchInternal(true, false);
  }
  
  @Test
  public void testGrokSyslogMatchInplaceTwoExpressions() throws Exception {
    testGrokSyslogMatchInternal(true, true);
  }
  
  private void testGrokSyslogMatchInternal(boolean inplace, boolean twoExpressions) throws Exception {
    // match
    morphline = createMorphline(
        "test-morphlines/grokSyslogMatch" 
        + (inplace ? "Inplace" : "")
        + (twoExpressions ? "TwoExpressions" : "") 
        + "");
    Record record = new Record();
    String msg = "<164>Feb  4 10:46:14 syslog sshd[607]: Server listening on 0.0.0.0 port 22.";
    record.put(Fields.MESSAGE, msg);
    String id = "myid";
    record.put(Fields.ID, id);
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));
    Record expected = new Record();
    expected.put(Fields.MESSAGE, msg);
    expected.put(Fields.ID, id);
    expected.put("syslog_pri", "164");
    expected.put("syslog_timestamp", "Feb  4 10:46:14");
    expected.put("syslog_hostname", "syslog");
    expected.put("syslog_program", "sshd");
    expected.put("syslog_pid", "607");
    expected.put("syslog_message", "Server listening on 0.0.0.0 port 22.");
    assertEquals(Arrays.asList(expected), collector.getRecords());
    if (inplace) {
      assertSame(record, collector.getRecords().get(0));
    } else {
      assertNotSame(record, collector.getRecords().get(0));      
    }
    
    // mismatch
    collector.reset();
    record = new Record();
    record.put(Fields.MESSAGE, "foo" + msg);
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertFalse(morphline.process(record));
    assertEquals(Arrays.asList(), collector.getRecords());
    
    // double match
    collector.reset();
    record = new Record();
    record.put(Fields.MESSAGE, msg);
    record.put(Fields.MESSAGE, msg);
    record.put(Fields.ID, id);
    record.put(Fields.ID, id);
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));
    Record tmp = expected.copy();
    for (Map.Entry<String, Object> entry : tmp.getFields().entries()) {
      expected.put(entry.getKey(), entry.getValue());
    }        
    assertEquals(Arrays.asList(expected), collector.getRecords());
    if (inplace) {
      assertSame(record, collector.getRecords().get(0));
    } else {
      assertNotSame(record, collector.getRecords().get(0));      
    }
  }
  
  @Test
  public void testGrokFindSubstrings() throws Exception {
    testGrokFindSubstringsInternal(false, false);
  }
  
  @Test
  public void testGrokFindSubstringsInplace() throws Exception {
    testGrokFindSubstringsInternal(true, false);
  }
  
  @Test
  public void testGrokFindSubstringsInplaceTwoExpressions() throws Exception {
    testGrokFindSubstringsInternal(true, true);
  }
  
  private void testGrokFindSubstringsInternal(boolean inplace, boolean twoExpressions) throws Exception {
    // match
    morphline = createMorphline(
        "test-morphlines/grokFindSubstrings" 
        + (inplace ? "Inplace" : "")
        + (twoExpressions ? "TwoExpressions" : "") 
        + "");
    Record record = new Record();
    String msg = "hello\t\tworld\tfoo";
    record.put(Fields.MESSAGE, msg);
    String id = "myid";
    record.put(Fields.ID, id);
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));
    Record expected = new Record();
    expected.put(Fields.MESSAGE, msg);
    expected.put(Fields.ID, id);
    expected.put("word", "hello");
    expected.put("word", "world");
    expected.put("word", "foo");
    assertEquals(Arrays.asList(expected), collector.getRecords());
    if (inplace) {
      assertSame(record, collector.getRecords().get(0));
    } else {
      assertNotSame(record, collector.getRecords().get(0));      
    }
    
    // mismatch
    collector.reset();
    record = new Record();
    record.put(Fields.MESSAGE, "");
    record.put(Fields.ID, id);
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertFalse(morphline.process(record));
    assertEquals(Arrays.asList(), collector.getRecords());
  }
  
  @Test
  public void testGrokSeparatedValues() throws Exception {
    String msg = "hello\tworld\tfoo";
    Pattern pattern = Pattern.compile("(?<word>.+?)(\\t|\\z)");
    Matcher matcher = pattern.matcher(msg);
    List<String> results = new ArrayList();
    while (matcher.find()) {
      //System.out.println("match:'" + matcher.group(1) + "'");
      results.add(matcher.group(1));
    }
    assertEquals(Arrays.asList("hello", "world", "foo"), results);
  }
  
  @Test
  public void testConvertTimestamp() throws Exception {
    morphline = createMorphline("test-morphlines/convertTimestamp");    
    Record record = new Record();
    record.put("ts1", "2011-09-06T14:14:34.789Z"); // "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
    record.put("ts1", "2012-09-06T14:14:34"); 
    record.put("ts1", "2013-09-06");
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));
    Record expected = new Record();
    expected.put("ts1", "2011-09-06T07:14:34.789-0700");
    expected.put("ts1", "2012-09-06T07:14:34.000-0700");
    expected.put("ts1", "2013-09-05T17:00:00.000-0700");
    assertEquals(Arrays.asList(expected), collector.getRecords());
    assertSame(record, collector.getRecords().get(0));
  }
  
  @Test
  public void testConvertTimestampEmpty() throws Exception {
    morphline = createMorphline("test-morphlines/convertTimestamp");
    Record record = new Record();
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));
    Record expected = new Record();
    assertEquals(Arrays.asList(expected), collector.getRecords());
    assertSame(record, collector.getRecords().get(0));
  }
  
  @Test
  public void testConvertTimestampBad() throws Exception {
    morphline = createMorphline("test-morphlines/convertTimestamp");
    Record record = new Record();
    record.put("ts1", "this is an invalid timestamp");
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertFalse(morphline.process(record));
    assertEquals(Arrays.asList(), collector.getRecords());
  }
  
  @Test
  public void testConvertTimestampWithDefaults() throws Exception {
    morphline = createMorphline("test-morphlines/convertTimestampWithDefaults");    
    Record record = new Record();
    record.put(Fields.TIMESTAMP, "2011-09-06T14:14:34.789Z");
    record.put(Fields.TIMESTAMP, "2012-09-06T14:14:34"); 
    record.put(Fields.TIMESTAMP, "2013-09-06");
    startSession();
    assertEquals(1, collector.getNumStartEvents());
    assertTrue(morphline.process(record));
    Record expected = new Record();
    expected.put(Fields.TIMESTAMP, "2011-09-06T14:14:34.789Z");
    expected.put(Fields.TIMESTAMP, "2012-09-06T14:14:34.000Z");
    expected.put(Fields.TIMESTAMP, "2013-09-06T00:00:00.000Z");
    assertEquals(Arrays.asList(expected), collector.getRecords());
    assertSame(record, collector.getRecords().get(0));
  }
  
  @Test
  public void testConvertTimestampWithBadTimezone() throws Exception {
    Config config = parse("test-morphlines/convertTimestampWithBadTimezone");    
    try {
      createMorphline(config);
      fail();
    } catch (MorphlineCompilationException e) {
      assertTrue(e.getMessage().startsWith("Unknown timezone"));
    }
  }
  
  @Test
  @Ignore
  public void testReflection() {
    long start = System.currentTimeMillis();
    List<String> packagePrefixes = Arrays.asList("com", "org", "net");
    for (Class clazz : new MorphlineContext().getTopLevelClassesRecursive(
        packagePrefixes, CommandBuilder.class)) {
      System.out.println("found " + clazz);
    }
    float secs = (System.currentTimeMillis() - start) / 1000.0f;
    System.out.println("secs=" + secs);
  }
  
  private Record createBasicRecord() {
    Record record = new Record();
    record.put("first_name", "Nadja");
    record.put("age", 8);
    record.put("tags", "one");
    record.put("tags", 2);
    record.put("tags", "three");
    return record;
  }

}
