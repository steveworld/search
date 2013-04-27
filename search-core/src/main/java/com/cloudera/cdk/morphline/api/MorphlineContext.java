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

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.cdk.morphline.shaded.com.google.common.reflect.ClassPath;
import com.cloudera.cdk.morphline.shaded.com.google.common.reflect.ClassPath.ClassInfo;
import com.google.common.base.Preconditions;
import com.yammer.metrics.core.MetricsRegistry;

/**
 * Additional user defined parameters that will be passed to all morphline commands.
 */
public class MorphlineContext {

  private MetricsRegistry metricsRegistry;
  private Map<String, Class<CommandBuilder>> commandBuilders = Collections.EMPTY_MAP;

  private static final Logger LOG = LoggerFactory.getLogger(MorphlineContext.class);

  /** For public access use {@link Builder#build()} instead */  
  protected MorphlineContext() {}
  
  public MetricsRegistry getMetricsRegistry() {
    assert metricsRegistry != null;
    return metricsRegistry;
  }

  public Class<CommandBuilder> getCommandBuilder(String builderName) {
    return commandBuilders.get(builderName);
  }

  public void importCommandBuilderPackagePrefixes(Collection<String> commandPackagePrefixes) {
    importCommandBuilders(
        getTopLevelClassesRecursive(commandPackagePrefixes, CommandBuilder.class));
  }

  public void importCommandBuilders(Collection<Class<CommandBuilder>> builderClasses) {
    if (commandBuilders == Collections.EMPTY_MAP) {
      commandBuilders = new HashMap();
      for (Class<CommandBuilder> builderClass : builderClasses) {
        try {
          CommandBuilder builder = builderClass.newInstance();
          for (String builderName : builder.getNames()) {
            LOG.info("Importing CommandBuilder named: {} for class: {}", builderName, builderClass.getName());
            if (builderName.contains(".")) {
              LOG.warn("CommandBuilder name should not contain a period character: " + builderName);
            }
            commandBuilders.put(builderName, builderClass);
          }
        } catch (Exception e) {
          throw new MorphlineRuntimeException(e);
        }
      }
    }
  }

  /**
   * Returns all classes that implement the given interface and are contained in a Java package with
   * the given prefix.
   * 
   * Uses a shaded version of com.google.guava.reflect-14.0.1 to enable running with prior
   * versions of guava without issues.
   */
  <T> Collection<Class<T>> getTopLevelClassesRecursive(Iterable<String> packageNamePrefixes, Class<T> iface) {    
    HashMap<String,Class<T>> classes = new LinkedHashMap();
    for (ClassLoader loader : getClassLoaders()) {
      ClassPath classPath;
      try {
        classPath = ClassPath.from(loader);
      } catch (IOException e) {
        continue;
      }
      for (String packageNamePrefix : packageNamePrefixes) {
        for (ClassInfo info : classPath.getTopLevelClassesRecursive(packageNamePrefix)) {
          Class clazz;
          try {
            clazz = info.load();
//            clazz = Class.forName(info.getName());
          } catch (NoClassDefFoundError e) {
            continue;
          } catch (ExceptionInInitializerError e) {
            continue;
          } catch (UnsatisfiedLinkError e) {
            continue;
          }
          if (!classes.containsKey(clazz.getName()) 
              && iface.isAssignableFrom(clazz) 
              && !clazz.isInterface()
              && !Modifier.isAbstract(clazz.getModifiers())) {
            classes.put(clazz.getName(), clazz);
          }
        }
      }
    }    
    return classes.values();
  }
  
  private ClassLoader[] getClassLoaders() {
    ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
    ClassLoader myLoader = getClass().getClassLoader();
    if (contextLoader == null) {
      return new ClassLoader[] { myLoader };
    } else if (contextLoader == myLoader || myLoader == null) {
      return new ClassLoader[] { contextLoader };
    } else {
      return new ClassLoader[] { contextLoader, myLoader };
    }
  }

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  /**
   * Helper to construct a {@link MorphlineContext} instance.
   * 
   * Example usage: 
   * 
   * <pre>
   * MorphlineContext context = new MorphlineContext.Builder().setMetricsRegistry(new MetricsRegistry()).build();
   * </pre>
   */
  public static class Builder {
    
    protected MorphlineContext context = create();
    private MetricsRegistry metricsRegistry;
    
    public Builder() {}

    public Builder setMetricsRegistry(MetricsRegistry metricsRegistry) {
      Preconditions.checkNotNull(metricsRegistry);
      this.metricsRegistry = metricsRegistry;
      return this;
    }

    public MorphlineContext build() {
      Preconditions.checkNotNull(metricsRegistry, "build() requires a prior call to setMetricsRegistry()");
      context.metricsRegistry = metricsRegistry;
      return context;
    }

    protected MorphlineContext create() {
      return new MorphlineContext();
    }
    
  }

}
