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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.yammer.metrics.core.MetricsRegistry;

/**
 * Additional user defined parameters that will be passed to all morphline commands.
 */
public class MorphlineContext {

  private final MetricsRegistry metricsRegistry;
  private Map<String, Class<CommandBuilder>> commandBuilders = Collections.EMPTY_MAP;

  private static final Logger LOG = LoggerFactory.getLogger(MorphlineContext.class);
      
  // TODO: use builder pattern to allow for more than a metricsRegistry to be added later without breaking the constructor API
  // also to pass a semi-immutable context instance to CommandBuilder.build()
  public MorphlineContext(MetricsRegistry metricsRegistry) {
    Preconditions.checkNotNull(metricsRegistry);
    this.metricsRegistry = metricsRegistry;
  }
  
  public MetricsRegistry getMetricsRegistry() {
    return metricsRegistry;
  }

  public Map<String, Class<CommandBuilder>> getCommandBuilders() {
    return commandBuilders;
  }

  public void registerCommandBuilderPackagePrefixes(Collection<String> commandPackagePrefixes) {
    registerCommandBuilders(
        new ClassPaths().getTopLevelClassesRecursive(commandPackagePrefixes, CommandBuilder.class));
  }

  public void registerCommandBuilders(Collection<Class<CommandBuilder>> builderClasses) {
    if (commandBuilders == Collections.EMPTY_MAP) {
      commandBuilders = new HashMap();
      for (Class<CommandBuilder> builderClass : builderClasses) {
        try {
          CommandBuilder builder = builderClass.newInstance();
          LOG.info("Registering CommandBuilder named: {} for class: {}", builder.getName(), builderClass.getName());
          if (builder.getName().contains(".")) {
            LOG.warn("CommandBuilder name should not contain a period character: " + builder.getName());
          }
          commandBuilders.put(builder.getName(), builderClass);
        } catch (Exception e) {
          throw new MorphlineRuntimeException(e);
        }
      }
    }
  }

}
