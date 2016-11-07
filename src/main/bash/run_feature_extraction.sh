#!/bin/bash
#
# Copyright (c) 2016 PayPal Corporation. All rights reserved.

# --------------------------------------------------------------------------------------------------------------
#
#
#   Author: haifwu@paypal.com
#
# --------------------------------------------------------------------------------------------------------------
# Environment variable
# --------------------------------------------------------------------------------------------------------------
source common_header.sh
# --------------------------------------------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------------------------------------------

# For an input folder, online data should in subfolder online data, offline data should in offline data
INPUT_PATH="CCF/test"
OUTPUT_PATH="ccf/featureExtraction"
# Two Option: online, offline
ONLINE_FOLDER_NAME="online"
OFFLINE_FOLDER_NAME="offline"

# --------------------------------------------------------------------------------------------------------------
# Functions
# --------------------------------------------------------------------------------------------------------------

# --------------------------------------------------------------------------------------------------------------
# Shell flow
# --------------------------------------------------------------------------------------------------------------

hadoop jar ${D_HOME}/lib/${JAR_NAME} com.rainyday.ccf.feature.mapreduce.MapReducerFeatureExtractionWorker \
        -Dline.separator=","  \
        -Dinput.path=${INPUT_PATH} \
        -Doutput.path=${OUTPUT_PATH} \
        -Donline.folder.name=${ONLINE_FOLDER_NAME} \
        -Doffline.folder.name=${OFFLINE_FOLDER_NAME} \
        -Dmapreduce.input.fileinputformat.input.dir.recursive=true \
        -Dmapred.job.queue.name=risk_platform

if [ $? == 0 ]; then
    echo "Job running successfully! You can find your output at ${OUTPUT_PATH}"
else
    echo "Job running failed! Please check your configure!"
fi