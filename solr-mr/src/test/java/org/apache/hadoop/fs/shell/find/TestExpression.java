/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.shell.find;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;

import org.apache.hadoop.fs.shell.find.Expression;

/**
 * Test utilities for unit testing the {@link Expression}s
 */
abstract class TestExpression {
  protected void addArgument(Expression expr, String arg) {
    expr.addArguments(new LinkedList<String>(Collections.singletonList(arg)));
  }
  protected LinkedList<String> getArgs(String cmd) {
    return new LinkedList<String>(Arrays.asList(cmd.split(" ")));
  }
}
