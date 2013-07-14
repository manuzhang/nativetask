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
import org.apache.hadoop.conf.Configuration;
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
  public static interface MapTaskDelegator {
    
    public void init(TaskUmbilicalProtocol umbilical, TaskReporter reporter, Configuration conf) 
        throws Exception;
    
    public void run(TaskAttemptID taskID,  
        Object split)
            throws IOException;
  }
  
  /**
   * inteface for map reduce task delegator
   *
   */
  public static interface ReduceTaskDelegator {

    public void init(TaskUmbilicalProtocol umbilical, TaskReporter reporter, Configuration conf) 
        throws Exception;
    
    public void run(TaskAttemptID taskID, 
        RawKeyValueIterator rIter, RawComparator comparator, 
        Class keyClass, Class valueClass)
        throws IOException;
  }

  /**
   * inteface for map output collector delegator
   *
   */
  public static interface MapOutputCollectorDelegator<K, V> extends
    MapTask.MapOutputCollector<K, V> {

    void init(TaskUmbilicalProtocol umbilical, TaskReporter reporter,
        Configuration conf, Task task) throws Exception;
  }
  
  public static MapTaskDelegator getMapTaskDelegator(TaskUmbilicalProtocol protocol
      , TaskReporter reporter, JobConf job) {
    String delegateMapClazz = job.get(MAP_TASK_DELEGATPR, null);
    if (null == delegateMapClazz || delegateMapClazz.isEmpty()) {
      LOG.info("MapTaskDelegator is not defined");
      return null;
    }
    Class<? extends MapTaskDelegator> delegatorClass = (Class<? extends MapTaskDelegator>) 
        job.getClass(MAP_TASK_DELEGATPR, null);
    if (null == delegatorClass) {
      LOG.info("MapTaskDelegator class cannot be load " + delegateMapClazz);
      return null;
    }
    MapTaskDelegator delegator = null;
    try {
      delegator = ReflectionUtils.newInstance(delegatorClass, job);
      delegator.init(protocol, reporter, job);
      LOG.info("MapTaskDelegator " + delegateMapClazz + " is enabled");
    }
    catch(Exception e) {
      LOG.error("MapTaskDelegator " + delegateMapClazz + " is not enabled", e);
      return null;
    }
    return delegator;
  }


  public static ReduceTaskDelegator getReduceTaskDelegator(TaskUmbilicalProtocol protocol
      , TaskReporter reporter, JobConf job) {
    String delegateReducerClazz = job.get(REDUCE_TASK_DELEGATPR, null);
    if (null == delegateReducerClazz || delegateReducerClazz.isEmpty()) {
      LOG.info("Reduce task Delegator not defined");
      return null;
    }
    Class<? extends ReduceTaskDelegator> delegatorClass = (Class<? extends ReduceTaskDelegator>) 
        job.getClass(REDUCE_TASK_DELEGATPR, null);
    if (null == delegatorClass) {
      LOG.info("Reduce task Delegator class cannot be load " + delegateReducerClazz);
      return null;
    }
    
    ReduceTaskDelegator delegator = null;
    try {
      delegator = ReflectionUtils.newInstance(delegatorClass, job);
      delegator.init(protocol, reporter, job);
      LOG.info("ReduceTaskDelegator " + delegateReducerClazz + " is enabled");
    }
    catch(Exception e) {
      LOG.warn("ReduceTaskDelegator " + delegateReducerClazz + " is not enabled", e);
      return null;
    }
    return delegator;
  }

  @SuppressWarnings("unchecked")
  public static <K, V> MapOutputCollectorDelegator<K, V> 
      getOutputCollectorDelegator(TaskUmbilicalProtocol protocol
          , TaskReporter reporter, JobConf job, Task task) {
    
    String delegatorClazz = job.get(MAP_OUTPUT_COLLECTOR_DELEGATPR, null);
    if (null == delegatorClazz || delegatorClazz.isEmpty()) {
      LOG.info("MapOutputCollectorDelegator not found");
      return null;
    }
    Class<? extends MapOutputCollectorDelegator> delegatorClass = (Class<? extends MapOutputCollectorDelegator>) 
        job.getClass(MAP_OUTPUT_COLLECTOR_DELEGATPR, null);
    if (null == delegatorClass) {
      LOG.info("MapOutputCollectorDelegator cannot be inited " + delegatorClazz);
      return null;
    }
    MapOutputCollectorDelegator delegator = null;
    try {
      delegator = ReflectionUtils.newInstance(delegatorClass, job);
      delegator.init(protocol, reporter, job, task);
      LOG.info("MapOutputCollectorDelegator " + delegatorClazz + " is enabled");
    }
    catch(Exception e) {
      LOG.error("MapOutputCollectorDelegator " + delegatorClazz + " is not enabled", e);
      return null;
    }
    return delegator;
  }
}
