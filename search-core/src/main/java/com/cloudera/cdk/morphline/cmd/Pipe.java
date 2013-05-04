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

import java.util.Arrays;
import java.util.List;

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.Configs;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.base.AbstractCommand;
import com.typesafe.config.Config;

/**
 * A morphline has a name and contains a chain of zero or more commands, through which the morphline
 * pipes each input record. A command transforms the record into zero or more records.
 */
final class Pipe extends AbstractCommand {
  
  private final String name;
  private final Command realChild;

  public Pipe(Config config, Command parent, Command child, MorphlineContext context) {
    super(config, parent, child, context);
    this.name = Configs.getString(config, "name");
    
    List<String> commandPackagePrefixes = Configs.getStringList(config, "importCommandPackagePrefixes", 
        Arrays.asList("com", "org", "net"));    
    context.importCommandBuilderPackagePrefixes(commandPackagePrefixes);
    
    List<Command> childCommands = buildCommandChain(config, "commands", child, false);
    if (childCommands.size() > 0) {
      this.realChild = childCommands.get(0);
    } else {
      this.realChild = child;
    }
  }
  
  @Override
  protected Command getChild() {
    return realChild;
  }
  
}
