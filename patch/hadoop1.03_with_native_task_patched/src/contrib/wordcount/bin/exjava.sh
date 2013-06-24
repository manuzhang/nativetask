#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

echo "========== running wordcount bench =========="
# configure
DIR=`cd $bin/../; pwd`
. "${DIR}/../bin/hibench-config.sh"
. "${DIR}/conf/configure.sh"

# compress
if [ $COMPRESS -eq 1 ]
then
    COMPRESS_OPT="-D mapred.output.compress=true \
    -D mapred.compress.map.output=true \
    -D mapred.output.compression.type=BLOCK \
    -D mapred.map.output.compression.codec=$COMPRESS_CODEC \
    -D mapred.output.compression.codec=$COMPRESS_CODEC"
else
    COMPRESS_OPT="-D mapred.output.compress=false"
fi

# path check
$HADOOP_HOME/bin/hadoop dfs -rmr  $OUTPUT_JAVA

# pre-running
SIZE=`$HADOOP_HOME/bin/hadoop fs -dus $INPUT_HDFS | awk '{ print $2 }'`
START_TIME=`timestamp`

# run bench
$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/hadoop-examples*.jar wordcount \
    -libjars $HADOOP_HOME/lib/hadoop-snappy-0.0.1-SNAPSHOT.jar,$HADOOP_HOME/lib/hadoop-nativetask-0.1.0.jar \
    $COMPRESS_OPT \
    -D mapred.job.name="ex_java" \
    -D native.mapoutput.collector.enabled=true \
    -D io.sort.mb=300 \
    -D mapred.reduce.tasks=32 \
    $INPUT_HDFS $OUTPUT_JAVA

#-Djava.library.path=$HADOOP_HOME/lib/native/Linux-amd64-64/ \
#    -D native.mapoutput.collector.enabled=true \
# post-running
END_TIME=`timestamp`
gen_report "WORDCOUNT" ${START_TIME} ${END_TIME} ${SIZE} >> ${HIBENCH_REPORT}
