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

source common_header.sh

# --------------------------------------------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------------------------------------------

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