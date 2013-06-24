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
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Task.TaskReporter;
import org.apache.hadoop.util.ReflectionUtils;

public class TaskDelegation {
  /**
   * Add setProgress interface, cause Reporter don't have this,
   * and TaskReporter is protected
   */
  private static final Log LOG = LogFactory.getLog(TaskDelegation.class);
  public static class DelegateReporter implements Reporter {
    private TaskReporter reporter;
    public DelegateReporter(TaskReporter reporter) {
      this.reporter = reporter;
    }
    @Override
    public void setStatus(String status) {
      reporter.setStatus(status);
    }
    @Override
    public void progress() {
      reporter.progress();
    }
    @Override
    public InputSplit getInputSplit() throws UnsupportedOperationException {
      return reporter.getInputSplit();
    }
    @Override
    public Counter getCounter(Enum<?> name) {
      return reporter.getCounter(name);
    }
    @Override
    public Counter getCounter(String group, String name) {
      return reporter.getCounter(group, name);
    }
    @Override
    public void incrCounter(Enum<?> key, long amount) {
      reporter.incrCounter(key, amount);
    }
    @Override
    public void incrCounter(String group, String counter, long amount) {
      reporter.incrCounter(group, counter, amount);
    }
    public void setProgress(float progress) {
      reporter.setProgress(progress);
    }
  }
  
  public static interface MapTaskDelegator {
    public void run(TaskAttemptID taskID, JobConf job,
        TaskUmbilicalProtocol umbilical, DelegateReporter reporter, Object split)
        throws IOException, InterruptedException;
  }

  public static boolean canDelegateMapTask(JobConf job) {
    return job.get("mapreduce.map.task.delegator.class", null) != null;
  }

  @SuppressWarnings("unchecked")
  public static void delegateMapTask(MapTask mapTask, JobConf job,
      TaskUmbilicalProtocol umbilical, TaskReporter reporter,
      Object split)
      throws IOException, InterruptedException {
    Class<? extends MapTaskDelegator> delegatorClass = (Class<? extends MapTaskDelegator>) 
        job.getClass("mapreduce.map.task.delegator.class", null);
    MapTaskDelegator delegator = ReflectionUtils.newInstance(delegatorClass, job);
    FileSplit fileSplit = (FileSplit)split;
    System.out.println("File split: " + fileSplit.getPath().toString());
      
    delegator.run(mapTask.getTaskID(), job, umbilical, new DelegateReporter(
        reporter), split);
  }

  public static interface ReduceTaskDelegator {
    public void run(TaskAttemptID taskID, JobConf job,
        TaskUmbilicalProtocol umbilical, DelegateReporter reporter,
        RawKeyValueIterator rIter) 
        throws IOException, InterruptedException;
  }

  public static boolean catDelegateReduceTask(JobConf job) {
    return job.get("mapreduce.reduce.task.delegator.class", null) != null;
  }

  @SuppressWarnings("unchecked")
  public static void delegateReduceTask(ReduceTask reduceTask, JobConf job,
      TaskUmbilicalProtocol umbilical, TaskReporter reporter,
      RawKeyValueIterator rIter) throws IOException, ClassNotFoundException,
      InterruptedException {
    Class<? extends ReduceTaskDelegator> delegatorClass = 
        (Class<? extends ReduceTaskDelegator>)job.getClass(
            "mapreduce.reduce.task.delegator.class", null);
    ReduceTaskDelegator delegator = ReflectionUtils
        .newInstance(delegatorClass, job);
    delegator.run(reduceTask.getTaskID(), job, umbilical, new DelegateReporter(
        reporter), rIter);
  }

  public interface MapOutputCollectorDelegator<K, V> extends
      MapTask.MapOutputCollector<K, V> {
  }

  @SuppressWarnings("unchecked")
  public static <K, V> MapTask.MapOutputCollector<K, V> 
      tryGetDelegateMapOutputCollector(JobConf job, TaskAttemptID taskId,
          MapOutputFile mapOutputFile) {
    try {
      Class<?> cls = Class.forName("org.apache.hadoop.mapred.nativetask.NativeMapOutputCollector");
      Method canEnbaleMthd = cls.getMethod("canEnable", JobConf.class);
      Boolean can = (Boolean)canEnbaleMthd.invoke(null, job);
      if (can) {
        Constructor<?> cons = cls.getConstructor(JobConf.class,
            TaskAttemptID.class);
        MapTask.MapOutputCollector<K, V> moc = 
            (MapTask.MapOutputCollector<K, V>) cons.newInstance(
                job, taskId);
        return moc;
      }
      return null;
    } catch (Exception e) {
      return null;
    }
  }
}
