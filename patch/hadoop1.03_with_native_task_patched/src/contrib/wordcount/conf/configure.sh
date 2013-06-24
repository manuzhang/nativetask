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

#!/bin/bash

# compress
COMPRESS=$COMPRESS_GLOBAL
COMPRESS_CODEC=$COMPRESS_CODEC_GLOBAL

# paths
INPUT_HDFS=${DATA_HDFS}/Wordcount/Input
OUTPUT_HDFS=${DATA_HDFS}/Wordcount/Output
OUTPUT_JAVA=${DATA_HDFS}/Wordcount/Outjava
OUTPUT_NATIVE=${DATA_HDFS}/Wordcount/Outnative
OUTPUT_HADOOP=${DATA_HDFS}/Wordcount/Outhadoop

if [ $COMPRESS -eq 1 ]; then
    INPUT_HDFS=${INPUT_HDFS}-comp
    OUTPUT_HDFS=${OUTPUT_HDFS}-comp
    OUTPUT_JAVA=${OUTPUT_JAVA}-comp
    OUTPUT_NATIVE=${OUTPUT_NATIVE}-comp
fi

# for preparation (per node) - 32G
#DATASIZE=32000000000
DATASIZE=32000000000
NUM_MAPS=16

# for running (in total)
NUM_REDS=48
