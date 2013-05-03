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

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.Record;

/**
 * Tools for notifications on the control plane.
 */
public final class Notifications {

  public static final String LIFE_CYLCLE = "lifecycle";
  
  public static List getLifeCycleEvents(Record notification) {
    return notification.get(LIFE_CYLCLE);
  }
  
  public static void notifyBeginTransaction(Command command) {
    Record notification = new Record();
    notification.put(LIFE_CYLCLE, LifeCycleEvent.beginTransaction);
    command.notify(notification);
  }
  
  public static void notifyCommitTransaction(Command command) {
    Record notification = new Record();
    notification.put(LIFE_CYLCLE, LifeCycleEvent.commitTransaction);
    command.notify(notification);
  }
  
  public static void notifyRollbackTransaction(Command command) {
    Record notification = new Record();
    notification.put(LIFE_CYLCLE, LifeCycleEvent.rollback);
    command.notify(notification);
  }
  
  public static void notifyShutdown(Command command) {
    Record notification = new Record();
    notification.put(LIFE_CYLCLE, LifeCycleEvent.shutdown);
    command.notify(notification);
  }
  
  public static void notifyStartSession(Command command) {
    Record notification = new Record();
    notification.put(LIFE_CYLCLE, LifeCycleEvent.startSession);
    command.notify(notification);
  }
  
  public static boolean contains(Record notification, LifeCycleEvent directive) {
    return notification.get(LIFE_CYLCLE).contains(directive);
  }
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  public static enum LifeCycleEvent {
    beginTransaction,
    commitTransaction,
    rollback,
    shutdown,
    startSession;
  }     

}
