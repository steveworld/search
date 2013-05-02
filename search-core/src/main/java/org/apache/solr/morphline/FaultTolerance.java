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
package org.apache.solr.morphline;

import org.apache.solr.client.solrj.SolrServerException;

import com.google.common.annotations.Beta;

/**
 * Mission critical, large-scale online production systems need to make progress without downtime
 * despite some issues.
 * 
 * Some program exceptions tend to be transient, in which case the corresponding task can be
 * retried. Examples include network connection errors, timeouts, etc. These are called recoverable
 * exceptions.
 * 
 * The isIgnoringRecoverableExceptions should only be enabled if an exception misclassification bug
 * has been identified.
 */
@Beta
public final class FaultTolerance {
    
  private final boolean isProductionMode; 
  private final boolean isIgnoringRecoverableExceptions;

  public FaultTolerance(boolean isProductionMode, boolean isIgnoringRecoverableExceptions) {
    this.isProductionMode = isProductionMode;
    this.isIgnoringRecoverableExceptions = isIgnoringRecoverableExceptions;
  }
  
  public boolean isProductionMode() {
    return isProductionMode;
  }
  
  public boolean isIgnoringRecoverableExceptions() {
    return isIgnoringRecoverableExceptions;
  }
  
  @Beta
  public boolean isRecoverableException(Throwable t) {
    while (true) {
      if (t instanceof SolrServerException) {
        return true;
      }
//        for (Class clazz : classes) {
//          if (clazz.isAssignableFrom(t.getClass())) {
//            return true;
//          }
//        }      
      Throwable cause = t.getCause();
      if (cause == null || cause == t) {
        return false;
      }
      t = cause;
    } 

  }
    
}
