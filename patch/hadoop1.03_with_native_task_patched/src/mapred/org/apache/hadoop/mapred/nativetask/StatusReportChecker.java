package org.apache.hadoop.mapred.nativetask;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.Task.Counter;
import org.apache.hadoop.mapred.TaskDelegation.DelegateReporter;

/**
 * Will periodically check status from native and report to MR framework.
 * 
 */
public class StatusReportChecker implements Runnable {

  private static Log LOG = LogFactory.getLog(StatusReportChecker.class);
  private Thread updaterThread;
  private DelegateReporter reporter;
  private long interval;

  public StatusReportChecker(DelegateReporter reporter) {
    this(reporter, 1000);
  }

  public StatusReportChecker(DelegateReporter reporter, long interval) {
    this.reporter = reporter;
    this.interval = interval;
  }

  @Override
  public void run() {
    while (true) {
      try {
        Thread.sleep(interval);
      } catch (InterruptedException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("StatusUpdater thread exiting "
              + "since it got interrupted");
        }
        break;
      }
      try {
        NativeRuntime.reportStatus(reporter);
      } catch (IOException e) {
        LOG.warn("Update native status got exception", e);
        reporter.setStatus(e.toString());
        break;
      }
    }
  }

  protected void initUsedCounters() {
    reporter.getCounter(Counter.MAP_INPUT_RECORDS);
    reporter.getCounter(Counter.MAP_OUTPUT_RECORDS);
    reporter.getCounter(Counter.MAP_INPUT_BYTES);
    reporter.getCounter(Counter.MAP_OUTPUT_BYTES);
    reporter.getCounter(Counter.MAP_OUTPUT_MATERIALIZED_BYTES);
    reporter.getCounter(Counter.COMBINE_INPUT_RECORDS);
    reporter.getCounter(Counter.COMBINE_OUTPUT_RECORDS);
    reporter.getCounter(Counter.REDUCE_INPUT_RECORDS);
    reporter.getCounter(Counter.REDUCE_OUTPUT_RECORDS);
    reporter.getCounter(Counter.REDUCE_INPUT_GROUPS);
  }

  public synchronized void startUpdater() {
    if (updaterThread == null) {
      // init counters used by native side,
      // so they will have correct display name
      initUsedCounters();
      updaterThread = new Thread(this);
      updaterThread.setDaemon(true);
      updaterThread.start();
    }
  }

  public synchronized void stopUpdater() throws InterruptedException {
    if (updaterThread != null) {
      updaterThread.interrupt();
      updaterThread.join();
    }
  }
}