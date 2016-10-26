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

export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$CLASSPATH:${D_HOME}/conf

# --------------------------------------------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------------------------------------------

JAR_NAME="bigdata-1.0-SNAPSHOT.jar"
INPUT_FILE="CCF/ccf_online_stage1_train.csv"
OUTPUT_PATH="ccf/online"
# Two Option: online, offline
INPUT_DATA_TYPE="online"

# --------------------------------------------------------------------------------------------------------------
# Functions
# --------------------------------------------------------------------------------------------------------------

# --------------------------------------------------------------------------------------------------------------
# Shell flow
# --------------------------------------------------------------------------------------------------------------

hadoop jar ${D_HOME}/lib/${JAR_NAME} com.rainyday.ccf.UserCouponCount  \
        -Dline.separator=","  \
        -Dinput.training.data.type=${INPUT_DATA_TYPE} \
        -Dinput.path=${INPUT_FILE} \
        -Doutput.path=${OUTPUT_PATH} \
        -Dmapred.job.queue.name=risk_platform

if [ $? == 0 ]; then
    echo "Job running successfully! You can find your output at ${OUTPUT_PATH}"
else
    echo "Job running failed! Please check your configure!"
fi