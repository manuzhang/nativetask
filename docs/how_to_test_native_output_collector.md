Steps to test native output collector.
===========
Required software

1. java

 hadoop need java 1.6, set your JAVA_HOME like `JAVA_HOME=/usr/lib/jvm/java-1.6.0-openjdk-1.6.0.0.x86_64`

1. maven

	need version later than 3.0.4. 
	If you need proxy, create settings.xml at ~/.m2/, and set it like
 
		<settings>
		    <proxies>
		     <proxy>
		        <active>true</active>
		        <protocol>http</protocol>
		        <host>your.proxy.url</host>
		        <port>port number</port>
		        <nonProxyHosts>sites.dont.need.proxy</nonProxyHosts>
		      </proxy>
		    </proxies>
		</settings> 
  
   For more details, please ref to http://maven.apache.org/guides/mini/guide-proxies.html 

1. ant

	version lower than 1.8 will case problem, we using v1.9.1.

	set your ANT_HOME like `ANT_HOME=/home/user/install/apache-ant-1.9.1`

	set ant proxy (if you need) `ANT_OPTS="-Dhttp.proxyHost=your.proxy.url -Dhttp.proxyPort=port number"`

===========

0. cd patch/hadoop1.03_with_native_task_patched/ ; get to working directory.
1. ant ; Build hadoop.jar.
2. ant examples ; Build hadoop-examples.jar.
3. ant compile-native ; Build libhadoop.so libnativetask.so, and libstreaming.so.
4. cp build/*.jar /usr/lib/hadoop/ ; Copy jar need by nativetask.
5. cp build/native/Linux-amd64-64/lib/*.so /usr/lib/hadoop/lib/native/Linux-amd64-64/ ; Copy native libraries.
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
