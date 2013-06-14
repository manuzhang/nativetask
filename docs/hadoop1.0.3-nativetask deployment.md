Preparation
------------

1.Download source of Hadoop1.0.3 using link below 
	http://svn.apache.org/repos/asf/hadoop/common/tags/release-1.0.3
2.Checkout source code of nativetask by git clone
	https://github.com/clockfly/nativetask.git

Compile Hadoop
--------------
1.Copy nativetask-branch-1.patch under nativetask/patch / to  dir of hadoop- 1.0.3.
2.Execute "patch p1 < nativetask-branch-1.patch" under dir of hadoop-1.0.3
3.Execute "ant compile-native" under dir of hadoop-1.0.3
4.Execute "cp -r build/native/Linux-amd64-64/* lib/native/Linux-amd64-64/*" (make sure  dir 	of lib/native/Linux-amd64-64 exists)
5.Execute "ant jar" 

Compile NativeTask
------------------
1.Update pom.xml under dir of nativetask, seek line below "<systemPath>/Users/decster/projects/hadoop-20-git/build/hadoop-core-0.20.205.1.jar</systemPath>",then update it to be "<systemPath>$HADOOP_HOME/build/hadoop-core-1.0.3-Intel.jar</systemPath>" (Here, hadoop-core-1.0.3-Intel.jar may be other name ,anyway it is under dir of $HADOOP_HOME/build)
2.compile nativetask, using command "mvn install", this operation needs network.

Add NativeTask to Hadoop
------------------------
1.Cp  target/native/target/usr/local/lib/*  $HADOOP/lib/
2.Download snappy http://code.google.com/p/snappy/
3.install snappy as 
	tar -zxcf  snappy-1.0.5.tar.gz
	cd snappy-1.0.5
	./configure --prefix=/usr/
	sudo make
	sudo make install
  verify installation
	echo "int main(){ return 0;}" > /tmp/a.c && gcc /tmp/a.c -o /tmp/a.out -lsnappy
	/tmp/a.out
  if there is no error ,it meas installation is ok


 now you can use hadoop with nativetask function.


Notice
------
This project is in very early stages currently, and is not well documented. 
If you are familiar with Hadoop MapReduce, you can hack into the source code. 
For more informantion, please read the 
[design document](https://github.com/decster/nativetask/wiki/The-Design-of-NativeTask)

Also you can find some discussion in Hadoop JIRA:  
https://issues.apache.org/jira/browse/MAPREDUCE-2841
