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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.CommandBuilder;
import com.cloudera.cdk.morphline.api.Configs;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.MorphlineRuntimeException;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.AbstractCommand;
import com.typesafe.config.Config;

/**
 * A filter consists of zero or more rules.
 * 
 * A rule consists of zero or more commands.
 * 
 * The rules of a filter are processed in top-down order. If one of the commands in a rule fails,
 * the filter stops processing of this rule, backtracks and tries the next rule, and so on, until a
 * rule is found that runs all its commands to completion without failure (the rule succeeds). If a
 * rule succeeds the remaining rules of the current filter are skipped. If no rule succeeds the
 * record remains unchanged, but a warning may be issued (the warning can be turned off) or an
 * exception may be thrown (which is logged and ignored in production mode).
 * 
 * Because a command can itself be a filter, there can be filters with actions, nested inside
 * filters, inside filters, recursively. This helps to implement arbitrarily complex functionality
 * for advanced usage.
 */
public final class FilterBuilder implements CommandBuilder {

  @Override
  public String getName() {
    return "filter";
  }
  
  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new Filter(config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class Filter extends AbstractCommand {

    private List<Command> childRules = new ArrayList();
    private boolean throwExceptionIfFoundNoMatchingRule;
    
    public Filter(Config config, Command parent, Command child, MorphlineContext context) {
      super(config, parent, child, context);
      this.throwExceptionIfFoundNoMatchingRule = 
          Configs.getBoolean(config, "throwExceptionIfFoundNoMatchingRule", true);
      
      List<? extends Config> ruleConfigs = Configs.getConfigList(config, "rules", Collections.EMPTY_LIST);
      for (Config ruleConfig : ruleConfigs) {
//        LOG.info("ruleConfig {}", ruleConfig);
        LOG.trace("ruleunwrapped {}", ruleConfig.root().unwrapped());
        List<Command> commands = buildCommandChain(ruleConfig, "commands", child, true);
        if (commands.size() > 0) {
          childRules.add(commands.get(0));
        }
      }
    }
    
    @Override
    public void startSession() {
      for (Command childRule : childRules) {
        childRule.startSession();
      }
      getChild().startSession();
    }
  
    @Override
    public boolean process(Record record) {
      for (Command childRule : childRules) {
        Record copy = record.copy();
//        try {
          if (childRule.process(copy)) {
            return true; // rule was executed successfully; no need to try the other remaining rules
          }
//        } catch (MorphlineRuntimeException e) {
//          LOG.warn("Filter rule exception", e);
//          // continue and try the other remaining rules
//        }
      }
      LOG.warn("Filter found no matching rule");
      if (throwExceptionIfFoundNoMatchingRule) {
        throw new MorphlineRuntimeException("Filter found no matching rule");
      }
      return false;
    }
    
  }
  
}
