Steps to test native output collector.
===========
0. Go to patch/hadoop1.03_with_native_task_patched/
1. ant; to build hadoop.jar.
2. ant examples; to build hadoop-examples.jar
3. ant compile-native to build libhadoop.so libnativetask.so, and libstreaming.so
4. Copy build/*.jar to /usr/lib/hadoop/
5. copy native libraries to /usr/lib/hadoop/lib/native/Linux-and64-64/. The native libraries is under build/native/Linux-amd64-64/lib/*.so
6. Restart namenode, datanode, jobtracker, and tasktracker.
7. Test whether native output collector is enabled by using:
<pre><code>hadoop jar hadoop-examples.jar pi -D 
mapreduce.map.output.collector.delegator.class=
org.apache.hadoop.mapred.nativetask.NativeMapOutputCollectorDelegator 3 100000</code></pre>
8. check the task Log, if there is 
``INFO org.apache.hadoop.mapred.nativetask.NativeMapOutputCollectorDelegator: Native output collector can be successfully enabled!``
Then it means the native task is successfully enabled.
