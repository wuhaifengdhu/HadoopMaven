#!/bin/bash
#
# Copyright (c) 2015 PayPal Corporation. All rights reserved.

# --------------------------------------------------------------------------------------------------------------
#
#
#   Author: haifwu@paypal.com
#
# --------------------------------------------------------------------------------------------------------------
#
# Functions
#
# --------------------------------------------------------------------------------------------------------------
# No functions
# --------------------------------------------------------------------------------------------------------------
#
# Shell flow
#
# --------------------------------------------------------------------------------------------------------------

BASH_HOME=$(cd `dirname $0`; pwd)
echo base home is $BASH_HOME
D_HOME=${BASH_HOME%/*}
echo d_home is $D_HOME

CLASSPATH=`find ${D_HOME}/lib -name "*.jar" | xargs | sed 's/ /:/g'`
LIBJARS=`find ${D_HOME}/lib -name "*.jar" | xargs | sed 's/ /,/g'`

export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$CLASSPATH:/etc/hbase/conf

hadoop jar ${D_HOME}/lib/bigdata-1.0-SNAPSHOT.jar com.haifwu.prince.TestHTableGetNoExsitValue