To enable native output collector, you need to point mapreduce.map.output.collector.delegator.class to buildin class
``mapreduce.map.output.collector.delegator.class=org.apache.hadoop.mapred.nativetask.NativeMapOutputCollectorDelegator``
 in jobconf.
  
Here is an example:  
``hadoop jar hadoop-examples.jar pi -D mapreduce.map.output.collector.delegator.class=
org.apache.hadoop.mapred.nativetask.NativeMapOutputCollectorDelegator 10 10  ``

Will use native output collector to calculate Pi.
