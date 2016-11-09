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
TRAINING_DATA_PATH="CCF/train"
FEATURE_OUTPUT_FOLDER="ccf/training/data/features"
FEATURE_USER="ccf/featureExtraction/USER_BEHAVIOUR/part-r-00000"
FEATURE_MERCHANT="ccf/featureExtraction/MERCHANT_BEHAVIOUR/part-r-00000"
FEATURE_COUPON="ccf/featureExtraction/COUPON_TENDENCY/part-r-00000"
# --------------------------------------------------------------------------------------------------------------
# Functions
# --------------------------------------------------------------------------------------------------------------

# --------------------------------------------------------------------------------------------------------------
# Shell flow
# --------------------------------------------------------------------------------------------------------------

# Step 1, Clear output folder
hadoop fs  -rm -r -f -skipTrash '${FEATURE_OUTPUT_FOLDER}' > /dev/null 2>&1

# Step 2, Run pig job
pig  -param JAR="${D_HOME}/lib/${JAR_NAME}"  \
     -param HADOOP_QUEUE="${JOB_QUEUE}"        \
     -param SOURCE="${TRAINING_DATA_PATH}"   \
     -param OUTPUT="${FEATURE_OUTPUT_FOLDER}"        \
     -param USER_DATA="${FEATURE_USER}"       \
     -param MERCHANT_DATA="${FEATURE_MERCHANT}"       \
     -param COUPON_DATA="${FEATURE_COUPON}"       \
     generate_training_features.pig