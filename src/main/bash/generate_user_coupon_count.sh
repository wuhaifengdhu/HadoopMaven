#!/bin/bash
#
# Copyright (c) 2016 HAIFENG WU. All rights reserved.
#
# --------------------------------------------------------------------------------------------------------------
#
#
#   Author: wuhaifengdhu@163.com
#
# --------------------------------------------------------------------------------------------------------------
# Environment variable
# --------------------------------------------------------------------------------------------------------------

BASH_HOME=$(cd `dirname $0`; pwd)
echo "BASH HOME is ${BASH_HOME}"
D_HOME=${BASH_HOME%/*}
echo "Directory HOME is ${D_HOME}"

CLASSPATH=`find ${D_HOME}/lib -name "*.jar" | xargs | sed 's/ /:/g'`
LIBJARS=`find ${D_HOME}/lib -name "*.jar" | xargs | sed 's/ /,/g'`

export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$CLASSPATH

# --------------------------------------------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------------------------------------------

JAR_NAME="bigdata-1.0-SNAPSHOT.jar"
INPUT_FILE=""
OUTPUT_PATH=""

# --------------------------------------------------------------------------------------------------------------
# Functions
# --------------------------------------------------------------------------------------------------------------

# --------------------------------------------------------------------------------------------------------------
# Shell flow
# --------------------------------------------------------------------------------------------------------------

hadoop jar ${D_HOME}/lib/{JAR_NAME} com.haifwu.prince.ActivityIdEmptyCheck  \
        -Dline.separator=","  \
        -Dinput.training.data.type="online" \
        -Dinput.path=${INPUT_FILE} \
        -Doutput.path=${OUTPUT_PATH} \
        -Dmapred.job.queue.name=risk_platform