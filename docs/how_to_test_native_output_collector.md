Steps to test native output collector.
===========
0. Go to patch/hadoop1.03_with_native_task_patched/
1. ant; to build hadoop.jar.
2. ant examples; to build hadoop-examples.jar
3. ant compile-native to build libhadoop.so libnativetask.so, and libstreaming.so
4. Copy build/*.jar to /usr/lib/hadoop/
5. copy native libraries to /usr/lib/hadoop/lib/native/Linux-and64-64/. The native libraries is under build/native/Linux-amd64-64/lib/*.so
6. Restart namenode, datanode, jobtracker, and tasktracker.
7. To enable native output collector, you need to point mapreduce.map.output.collector.delegator.class to buildin class
``mapreduce.map.output.collector.delegator.class=org.apache.hadoop.mapred.nativetask.NativeMapOutputCollectorDelegator``
 in jobconf.
  
8. Here is an example:  
<pre><code>hadoop jar hadoop-examples.jar pi -D mapreduce.map.output.collector.delegator.class=
org.apache.hadoop.mapred.nativetask.NativeMapOutputCollectorDelegator 10 10  </code></pre>

Will use native output collector to calculate Pi.

9. check the task Log, if there is 
``INFO org.apache.hadoop.mapred.nativetask.NativeMapOutputCollectorDelegator: Native output collector can be successfully enabled!``
Then it means the native task is successfully enabled.

Examples:
=========================
WordCount:
------------------------
As it don't support IntWritable, we have do a little modification of the original wordcount to use BytesWritable instead.
<pre>
<code>
prepare the data:
bin/hadoop jar hadoop-examples-1.0.3-Intel.jar randomtextwriter 
-Dtest.randomtextwrite.total_bytes=100000 -Dtest.randomtextwrite.bytes_per_map=100000  
-outFormat org.apache.hadoop.mapred.TextOutputFormat /text_wordcount_input

run word count:
bin/hadoop jar hadoop-examples-1.0.3-Intel.jar wordcount  
-Dmapreduce.map.output.collector.delegator.class=org.apache.hadoop.mapred.nativetask.NativeMapOutputCollectorDelegator 
/text_wordcount_input /text_wordcount_output 
</code>
</pre>

Terasort
-------------------------
<pre>
<code>
prepare the data:
bin/hadoop jar hadoop-examples-1.0.1-SNAPSHOT.jar teragen 1000 /tera100k-snappy 

run tera sort:
bin/hadoop jar hadoop-examples-1.0.1-SNAPSHOT.jar terasort 
-Dmapreduce.map.output.collector.delegator.class=org.apache.hadoop.mapred.nativetask.NativeMapOutputCollectorDelegator
/tera100k-snappy /terasort100k-facebook-output

</code>
</pre>

