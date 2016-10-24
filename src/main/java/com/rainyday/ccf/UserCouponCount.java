package com.rainyday.ccf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author haifwu
 */
public class UserCouponCount {
    private static final Logger LOG = LoggerFactory.getLogger(UserCouponCount.class);

    enum TrainingDataType {
        ONLINE, OFFLINE
    }

    enum RunningCount {
        RECORD_FORMAT_ERROR, ONLINE_USE_COUPON_COUNT, OFFLINE_USE_COUPON_COUNT
    }

    public static class CountUserMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static String lineSeparator;
        private static TrainingDataType trainingDataType;
        private static Text outKey = new Text();
        private static IntWritable one = new IntWritable(1);

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            lineSeparator = conf.get(CcfConstants.LINE_SEPARATOR_KEY, CcfConstants.DEFAULT_LINE_SEPARATOR);
            String inputType = conf.get(CcfConstants.INPUT_TRAINING_DATA_TYPE);
            if (null == inputType) {
                LOG.error("Input training data type not set!");
            } else if (inputType.equalsIgnoreCase(TrainingDataType.OFFLINE.toString())) {
                trainingDataType = TrainingDataType.OFFLINE;
            } else if (inputType.equalsIgnoreCase(TrainingDataType.ONLINE.toString())) {
                trainingDataType = TrainingDataType.ONLINE;
            } else {
                LOG.error("Input type " + inputType + " are not supported.");
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (null != line) {
                String[] records = value.toString().split(lineSeparator);
                switch (trainingDataType) {
                    case OFFLINE:
                        WriteOfflineData(records, context);
                        break;
                    case ONLINE:
                        WriteOnlineData(records, context);
                        break;
                }
            }
        }

        /**
         * Calculate online user use coupon history.
         * online records format: User_id, Merchant_id, Action, Coupon_id, Discount_rate, Date_received, Date
         *
         * @param records input line split value
         * @param context map context
         */
        private void WriteOnlineData(String[] records, Context context) throws IOException, InterruptedException {
            if (null == records || records.length != 7) {
                context.getCounter(RunningCount.RECORD_FORMAT_ERROR).increment(1L);
                LOG.error("input line: [" + ( null == records ? "" : records.toString() ) + "] records format error!");
                return;
            }
            String userId = records[0];
            String couponId = records[3];
            String date = records[6];
            if(null != userId && null != couponId && null != date){
                outKey.set(userId);
                context.write(outKey, one);
                context.getCounter(RunningCount.ONLINE_USE_COUPON_COUNT).increment(1L);
            }
        }

        /**
         * Calculate offline user use coupon history.
         * offline records format: User_id, Merchant_id, Coupon_id, Discount_rate, Distance, Date_received, Date
         *
         * @param records input line split value
         * @param context map context
         */
        private void WriteOfflineData(String[] records, Context context) throws IOException, InterruptedException {
            if (null == records || records.length != 7) {
                context.getCounter(RunningCount.RECORD_FORMAT_ERROR).increment(1L);
                LOG.error("input line: [" + ( null == records ? "" : records.toString() ) + "] records format error!");
                return;
            }
            String userId = records[0];
            String couponId = records[2];
            String date = records[6];
            if(null != userId && null != couponId && null != date){
                outKey.set(userId);
                context.write(outKey, one);
                context.getCounter(RunningCount.OFFLINE_USE_COUPON_COUNT).increment(1L);
            }
        }

        private boolean inputParametersNotRight() {
            if (null == trainingDataType) {
                return true;
            }
            return false;
        }

        @Override
        public void run(Context context) throws IOException, InterruptedException {
            setup(context);
            if (inputParametersNotRight()) {
                LOG.error("Input parameter not right!");
                return;
            }
            try {
                while (context.nextKeyValue()) {
                    map(context.getCurrentKey(), context.getCurrentValue(), context);
                }
            } finally {
                cleanup(context);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        Configuration conf = new Configuration();
        new GenericOptionsParser(conf, args);
        Path inputPath = new Path(conf.get(CcfConstants.INPUT_PATH));
        Path outputPath = new Path(conf.get(CcfConstants.OUTPUT_PATH));
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf, "UserCouponCount");
        job.setJarByClass(UserCouponCount.class);
        job.setMapperClass(UserCouponCount.CountUserMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, inputPath);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, outputPath);

        int code = job.waitForCompletion(true) ? 0 : 1;
        if (code != 0) {
            LOG.error("UserCouponCount job running failed!");
        }
        LOG.info("UserCouponCount Job running succeeded! Total time: {}ms.", System.currentTimeMillis() - startTime);
        System.exit(code);
    }
}
