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

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.cloudera.cdk.morphline.api.MorphlineParsingException;
import com.cloudera.cdk.morphline.api.Record;
import com.google.common.base.Joiner;
import com.typesafe.config.Config;

/**
 * Helper to fetch the values of a field of a {@link Record} referred to by a dynamic reference,
 * which is a String of the form <code>@{fieldname}</code>.
 */
public class FieldReferenceResolver {
  
  // example: @{first_name}
  private static final Pattern PATTERN = Pattern.compile("@\\{.*?\\}");
  
  // TODO: optimize by not using regexes
  public List resolveReference(String reference, Record record, Config config) {
    Matcher matcher = PATTERN.matcher(reference);
    if (!matcher.matches()) {
      throw new MorphlineParsingException("Invalid variable reference", config);
    }
    String value = reference.substring("@{".length(), reference.length() - "}".length());
    List resolvedValues = record.getFields().get(value);
    return resolvedValues;
  }

  // TODO: optimize by not using regexes
  public String resolveExpression(String expr, Record record) {
    Matcher matcher = PATTERN.matcher(expr);    
    StringBuilder buf = new StringBuilder();
    int from = 0;
    while (matcher.find()) {
      int start = matcher.start();
      int end = matcher.end();
      buf.append(expr.substring(from, start));
      String ref = expr.substring(start + "@{".length(), end - "}".length());
      if (ref.length() == 0) {
        buf.append(record.toString()); // @{} means dump string representation of entire record 
      } else {
        List resolvedValues = record.getFields().get(ref);
        Joiner.on(" ").appendTo(buf, resolvedValues);
      }
      from = end;
    }
    buf.append(expr.substring(from, expr.length()));
    return buf.toString();
  }

}