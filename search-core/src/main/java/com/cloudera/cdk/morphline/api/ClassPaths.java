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
import java.util.HashMap;
import java.util.LinkedHashMap;

import com.cloudera.cdk.morphline.shaded.com.google.common.reflect.ClassPath;
import com.cloudera.cdk.morphline.shaded.com.google.common.reflect.ClassPath.ClassInfo;

/**
 * Uses a repackaged version of com.google.guava.reflect-14.0.1 to enable running with prior
 * versions of guava without issues.
 */
final class ClassPaths {
  
  public <T> Collection<Class<T>> getTopLevelClassesRecursive(Iterable<String> packageNamePrefixes, Class<T> iface) {    
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

}