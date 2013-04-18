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

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.cdk.morphline.base.FieldReferenceResolver;
import com.typesafe.config.ConfigFactory;

public class FieldReferenceResolverTest extends Assert {
  
  @Test
  public void testSimplePatterns() throws Exception {
    //             012345678901234567890123456789
    String expr = "Mr. @{first_name} age: @{age}";
    String regex = "@\\{(.*?)\\}";
    Matcher matcher = Pattern.compile(regex).matcher(expr);
    assertTrue(matcher.find());
    assertEquals("first_name", matcher.group(1));
    assertEquals(6, matcher.start(1));
    assertEquals(6 + "first_name".length(), matcher.end(1));
    assertTrue(matcher.find());
    assertEquals("age", matcher.group(1));
    assertEquals(25, matcher.start(1));
    assertEquals(25 + "age".length(), matcher.end(1));
    assertFalse(matcher.find());    
    
    matcher = Pattern.compile("foo").matcher("foo");
    assertTrue(matcher.matches());
    matcher = Pattern.compile("foo").matcher("barfoo");
    assertFalse(matcher.matches());
    matcher = Pattern.compile(".*foo").matcher("barfoo");
    assertTrue(matcher.matches());
  }
  
  @Test
  public void testBasic() throws Exception {
    Record record = new Record();
    record.getFields().put("first_name", "Nadja");
    record.getFields().put("age", 8);
    record.getFields().put("tags", "one");
    record.getFields().put("tags", 2);
    record.getFields().put("tags", "three");
    
    assertEquals("foo", resolveExpression("foo", record));
    assertEquals("", resolveExpression("", record));
    assertEquals("Nadja", resolveExpression("@{first_name}", record));
    assertEquals("Ms. Nadja", resolveExpression("Ms. @{first_name}", record));
    assertEquals("Ms. Nadja is 8 years old.", resolveExpression("Ms. @{first_name} is @{age} years old.", record));
    
    assertEquals(Arrays.asList("Nadja"), resolveReference("@{first_name}", record));
    assertEquals(Arrays.asList("one", 2, "three"), resolveReference("@{tags}", record));
    try {
      resolveReference("first_name", record);
      fail();
    } catch (MorphlineParsingException e) {
      ;
    }
  }
  
  private Object resolveExpression(String expr, Record record) {
    return new FieldReferenceResolver().resolveExpression(expr, record);
  }
  
  private List resolveReference(String expr, Record record) {
    return new FieldReferenceResolver().resolveReference(expr, record, ConfigFactory.empty());
  }
  
}
