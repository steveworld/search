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
package com.cloudera.cdk.morphline.cmd;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.AbstractCommand;
import com.cloudera.cdk.morphline.base.FieldExpression;
import com.typesafe.config.Config;

/**
 * Base class for addValues/setValues/addValuesIfAbsent commands.
 */
abstract class AbstractAddValuesCommand extends AbstractCommand {
  
  private final Set<Map.Entry<String, Object>> entrySet;
  
  public AbstractAddValuesCommand(Config config, Command parent, Command child, MorphlineContext context) {
    super(config, parent, child, context);      
    entrySet = config.root().unwrapped().entrySet();
  }
      
  @Override
  public boolean process(Record record) { 
    for (Map.Entry<String, Object> entry : entrySet) {
      String fieldName = entry.getKey();
      prepare(record, fieldName);
      Object entryValue = entry.getValue();
      if (entryValue instanceof String) {
        List results = new FieldExpression((String) entryValue, getConfig()).evaluate(record);
        putAll(record, fieldName, results);
      } else if (entryValue instanceof List) {
        for (Object value : (List)entryValue) {
          if (value instanceof String) {
            List results = new FieldExpression((String) value, getConfig()).evaluate(record);
            putAll(record, fieldName, results);
          } else {
            put(record, fieldName, value);
          }
        }
      } else {
        put(record, fieldName, entryValue);
      }
    }
    return super.process(record);
  }
  
  protected void prepare(Record record, String key) {    
  }
  
  protected void putAll(Record record, String key, Collection values) {
    record.getFields().putAll(key, values);
  }
  
  protected void put(Record record, String key, Object value) {
    record.getFields().put(key, value);
  }
  
}
