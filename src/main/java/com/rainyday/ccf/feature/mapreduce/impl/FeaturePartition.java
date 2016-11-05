package com.rainyday.ccf.feature.mapreduce.impl;

import com.rainyday.ccf.feature.container.extractable.FeatureType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author haifwu
 */
public class FeaturePartition extends Partitioner<Text, Text> {
    private static final Logger LOG = LoggerFactory.getLogger(FeaturePartition.class);

    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
        /**
         * For all key string not in FeatureType will be partition to the last reducer.
         */
        int partition = numPartitions > 1 ? numPartitions - 1 : 0;
        try {
            FeatureType type = FeatureType.valueOf(key.toString());
            partition = type.ordinal();
        } catch (IllegalArgumentException ignore) {
            LOG.error("Invalid key for partition! Key:" + key.toString());
        }
        return partition;
    }
}
