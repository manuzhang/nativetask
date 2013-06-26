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

package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.Task.TaskReporter;
import org.apache.hadoop.util.ReflectionUtils;

public class TaskDelegation {

  private static final Log LOG = LogFactory.getLog(TaskDelegation.class);
  
  public final static String MAP_TASK_DELEGATPR = "mapreduce.map.task.delegator.class";
  public final static String REDUCE_TASK_DELEGATPR = "mapreduce.reduce.task.delegator.class";
  public final static String MAP_OUTPUT_COLLECTOR_DELEGATPR = "mapreduce.map.output.collector.delegator.class";
  

  /**
   * inteface for map task delegator
   *
   */
  public static interface MapTaskDelegator extends Configurable {
    
    public void run(TaskAttemptID taskID,  
        TaskUmbilicalProtocol umbilical, TaskReporter reporter,
        Object split)
            throws IOException;
  }
  
  /**
   * inteface for map reduce task delegator
   *
   */
  public static interface ReduceTaskDelegator extends Configurable {

    public void run(TaskAttemptID taskID,  
        TaskUmbilicalProtocol umbilical, TaskReporter reporter, 
        RawKeyValueIterator rIter, RawComparator comparator, 
        Class keyClass, Class valueClass)
        throws IOException;
  }

  /**
   * inteface for map output collector delegator
   *
   */
  public static interface MapOutputCollectorDelegator<K, V> extends
    MapTask.MapOutputCollector<K, V>, Configurable {
  }
  
  public static MapTaskDelegator getMapTaskDelegator(JobConf job) {
    String delegateMapClazz = job.get(MAP_TASK_DELEGATPR, null);
    if (null == delegateMapClazz || delegateMapClazz.isEmpty()) {
      return null;
    }
    Class<? extends MapTaskDelegator> delegatorClass = (Class<? extends MapTaskDelegator>) 
        job.getClass(delegateMapClazz, null);
    if (null == delegatorClass) {
      return null;
    }
    MapTaskDelegator delegator = null;
    try {
      delegator = ReflectionUtils.newInstance(delegatorClass, job);
      LOG.info("MapTaskDelegator " + delegateMapClazz + " is enabled");
    }
    catch(RuntimeException e) {
      LOG.error("MapTaskDelegator " + delegateMapClazz + " is not enabled", e);
    }
    return delegator;
  }


  public static ReduceTaskDelegator getReduceTaskDelegator(JobConf job) {
    String delegateReducerClazz = job.get(REDUCE_TASK_DELEGATPR, null);
    if (null == delegateReducerClazz || delegateReducerClazz.isEmpty()) {
      return null;
    }
    Class<? extends ReduceTaskDelegator> delegatorClass = (Class<? extends ReduceTaskDelegator>) 
        job.getClass(delegateReducerClazz, null);
    if (null == delegatorClass) {
      return null;
    }
    
    ReduceTaskDelegator delegator = null;
    try {
      delegator = ReflectionUtils.newInstance(delegatorClass, job);
      LOG.info("ReduceTaskDelegator " + delegateReducerClazz + " is enabled");
    }
    catch(RuntimeException e) {
      LOG.warn("ReduceTaskDelegator " + delegateReducerClazz + " is not enabled", e);
    }
    return delegator;
  }

  @SuppressWarnings("unchecked")
  public static <K, V> MapOutputCollectorDelegator<K, V> 
      getOutputCollectorDelegator(JobConf job) {
    
    String delegatorClazz = job.get(MAP_OUTPUT_COLLECTOR_DELEGATPR, null);
    if (null == delegatorClazz || delegatorClazz.isEmpty()) {
      return null;
    }
    Class<? extends MapOutputCollectorDelegator> delegatorClass = (Class<? extends MapOutputCollectorDelegator>) 
        job.getClass(delegatorClazz, null);
    if (null == delegatorClass) {
      return null;
    }
    MapOutputCollectorDelegator delegator = null;
    try {
      delegator = ReflectionUtils.newInstance(delegatorClass, job);
      LOG.info("MapOutputCollectorDelegator " + delegatorClazz + " is enabled");
    }
    catch(RuntimeException e) {
      LOG.error("MapOutputCollectorDelegator " + delegatorClazz + " is not enabled", e);
    }
    return delegator;
  }
}
