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
FEATURE_OUTPUT_FOLDER="ccfNew/training/data/features33"
FEATURE_USER="ccfNew/featureExtraction/USER_BEHAVIOUR/part-r-00000"
FEATURE_MERCHANT="ccfNew/featureExtraction/MERCHANT_BEHAVIOUR/part-r-00000"
FEATURE_COUPON="ccfNew/featureExtraction/COUPON_TENDENCY/part-r-00000"
FEATURE_USER_MERCHANT="ccfNew/featureExtraction/USER_MERCHANT/part-r-00000"
FEATURE_USER_COUPON="ccfNew/featureExtraction/USER_COUPON/part-r-00000"
FEATURE_USER_DISCOUNT="ccfNew/featureExtraction/USER_DISCOUNT/part-r-00000"
FEATURE_USER_DISTANCE="ccfNew/featureExtraction/USER_DISTANCE/part-r-00000"
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
     -param COUPON_DATA="${FEATURE_COUPON}"          \
     -param USER_MERCHANT_DATA="${FEATURE_USER_MERCHANT}"   \
     -param USER_COUPON_DATA="${FEATURE_USER_COUPON}"     \
     -param USER_DISCOUNT_DATA="${FEATURE_USER_DISCOUNT}"   \
     -param USER_DISTANCE_DATA="${FEATURE_USER_DISTANCE}"   \
     generate_training_features.pig