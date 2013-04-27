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
package com.cloudera.cdk.morphline.base;

import java.util.ArrayList;
import java.util.List;

import com.cloudera.cdk.morphline.api.Record;

/**
 * Helper to fetch the values of a field of a {@link Record} referred to by a field expression,
 * which is a String of the form <code>@{fieldname}</code>.
 */
public final class FieldExpressions {
  
  public static List evaluate(String expression, Record record) {
    ArrayList results = new ArrayList(1);
    evaluate(0, expression, record, new StringBuilder(), results);
    return results;
  }

  private static void evaluate(int from, String expr, Record record, StringBuilder buf, ArrayList results) {
    String START_TOKEN = "@{";
    char END_TOKEN = '}';
    int start = expr.indexOf(START_TOKEN, from);
    if (start < 0) { // START_TOKEN not found
      if (from == 0) {
        results.add(expr); // fast path
      } else {
        buf.append(expr, from, expr.length());
        results.add(buf.toString());
      }
    } else { // START_TOKEN found
      int end = expr.indexOf(END_TOKEN, start + START_TOKEN.length());
      if (end < 0) {
        throw new IllegalArgumentException("Missing closing token: " + END_TOKEN);
      }
      buf.append(expr, from, start);
      String ref = expr.substring(start + START_TOKEN.length(), end);
      if (ref.length() == 0) {
        buf.append(record.toString()); // @{} means dump string representation of entire record
        evaluate(end + 1, expr, record, buf, results);
      } else {
        List resolvedValues = record.getFields().get(ref);
        if (start == 0 && end + 1 == expr.length()) { 
          results.addAll(resolvedValues); // "@{first_name}" resolves to object list rather than string concat
        } else {
          for (Object value : resolvedValues) {
            StringBuilder buf2 = new StringBuilder(buf);
            buf2.append(value.toString());
            evaluate(end + 1, expr, record, buf2, results);
          }
        }
      }
    }
  }
  
}