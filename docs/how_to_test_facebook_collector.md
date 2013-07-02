Steps to test facebook collector for map output
======================
0. Go to patch/hadoop1.03_with_native_task_patched/
1. ant; to build hadoop.jar.
2. ant examples; to build hadoop-examples.jar
3. ant compile-native to build libhadoop.so libnativetask.so, and libstreaming.so
4. Copy build/*.jar to /usr/lib/hadoop/
5. copy native libraries to /usr/lib/hadoop/lib/native/Linux-and64-64/. The native libraries is under build/native/Linux-amd64-64/lib/*.so
6. Restart namenode, datanode, jobtracker, and tasktracker.
7. Test whether native output collector is enabled by defining flag mapreduce.map.output.collector.delegator.class=
org.apache.hadoop.mapred.BlockMapOutputBuffer
8. But current it only support BytesWriteable key and BytesWritable value.

Here is an example:

<pre><code>

You cannot use this directly. As we only support key value type as BytesWritable. 

hadoop jar hadoop-examples.jar pi -D mapreduce.map.output.collector.delegator.class=
org.apache.hadoop.mapred.BlockMapOutputBuffer 3 100000</code></pre>
9. check the task Log, if there is 
``MapOutputCollectorDelegator BlockMapOutputBuffer is enabled!``
Then it means the facebook collector task is successfully enabled.
