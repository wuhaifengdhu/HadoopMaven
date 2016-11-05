package com.rainyday.ccf.feature.mapreduce;

import com.rainyday.ccf.feature.container.data.AbstractData;
import com.rainyday.ccf.feature.container.data.DataType;
import com.rainyday.ccf.feature.container.data.OfflineTrainingData;
import com.rainyday.ccf.feature.container.data.OnlineTrainingData;
import com.rainyday.ccf.feature.container.extractable.FeatureExtractionProducer;
import com.rainyday.ccf.feature.container.extractable.FeatureType;
import com.rainyday.ccf.feature.mapreduce.impl.FeatureOutputFormat;
import com.rainyday.ccf.feature.mapreduce.impl.FeaturePartition;
import com.rainyday.ccf.feature.util.CcfConstants;
import com.rainyday.ccf.feature.util.CcfUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.AbstractMap;

/**
 * @author haifwu
 */
public class MapReducerFeatureExtractionWorker {
    private static final Logger LOG = LoggerFactory.getLogger(MapReducerFeatureExtractionWorker.class);

    /**
     * line separator used to cut the line
     */
    private static String lineSeparator;

    /**
     * We divided data type according to files' parent folder name
     */
    private static String onlineFolderName;
    private static String offlineFolderName;

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

        /**
         * get configure parameters form
         */
        lineSeparator = conf.get(CcfConstants.LINE_SEPARATOR_KEY, CcfConstants.DEFAULT_LINE_SEPARATOR);
        onlineFolderName = conf.get(CcfConstants.ONLINE_FOLDER_NAME_PARA, CcfConstants.DEFAULT_ONLINE_FOLDER_NAME);
        offlineFolderName = conf.get(CcfConstants.OFFLINE_FOLDER_NAME_PARA, CcfConstants
                .DEFAULT_OFFLINE_FOLDER_NAME);

        // Setting job information
        Job job = Job.getInstance(conf, "MapReducerFeatureExtractionWorker");
        job.setJarByClass(MapReducerFeatureExtractionWorker.class);
        job.setMapperClass(MapReducerFeatureExtractionWorker.FeatureExtractionMapper.class);
        job.setReducerClass(MapReducerFeatureExtractionWorker.FeatureExtractionReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        //TODO we should add files only parent path name is 'online' or 'offline'
        FileInputFormat.addInputPath(job, inputPath);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, outputPath);
        // Set partition
        job.setPartitionerClass(FeaturePartition.class);
        job.setNumReduceTasks(FeatureType.values().length + 1); // The last reduce task for unknown key partition

        int code = job.waitForCompletion(true) ? 0 : 1;
        if (code != 0) {
            LOG.error(CcfConstants.LOG_PREFIX + "MapReducerFeatureExtractionWorker job running failed!");
        }
        LOG.info("MapReducerFeatureExtractionWorker Job running succeeded! Total time: {}s.", (System
                .currentTimeMillis() - startTime)/1000);
        System.exit(code);
    }

    private static FeatureType getFeatureType(String feature) {
        if(null == feature) return null;
        if(FeatureType.USER_BEHAVIOUR.toString().equals(feature)){
            return FeatureType.USER_BEHAVIOUR;
        } else if(FeatureType.MERCHANT_BEHAVIOUR.toString().equals(feature)){
            return FeatureType.MERCHANT_BEHAVIOUR;
        } else if(FeatureType.COUPON_TENDENCY.toString().equals(feature)){
            return FeatureType.COUPON_TENDENCY;
        }
        return null;
    }

    enum RunningCount {
        RECORD_FORMAT_ERROR, USE_COUPON_COUNT, NOT_USE_COUPON_COUNT, INPUT_PARAMETER_ERROR
    }

    enum OnlineColumn {
        USER_ID, MERCHANT_ID, COUPON_ID, DISCOUNT_RATE,
    }

    private static class FeatureExtractionMapper extends Mapper<LongWritable, Text, Text, Text> {
        /**
         * key, value for map output
         */
        private static Text outKey = new Text();
        private static Text outValue = new Text();
        /**
         * dataType according to input file folder name
         */
        private static DataType dataType;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String parentFolderName = ((FileSplit) context.getInputSplit()).getPath().getParent().getName();
            if(null != parentFolderName){
                if(parentFolderName.equals(onlineFolderName)){
                    dataType = DataType.ONLINE;
                } else if(parentFolderName.equals(offlineFolderName)){
                    dataType = DataType.OFFLINE;
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            AbstractData abstractData = null;
            String line = value.toString();
            if(DataType.ONLINE.equals(dataType)){
                abstractData = new OnlineTrainingData(line, lineSeparator).build();
            } else if(DataType.OFFLINE.equals(dataType)){
                abstractData = new OfflineTrainingData(line, lineSeparator).build();
            }
            if(null != abstractData){
                FeatureExtractionProducer producer = new FeatureExtractionProducer(abstractData);
                if(null == producer){
                    LOG.error("Created FeatureExtractionProducer null, please check, value:" + line);
                    return;
                }
                while(producer.hasNext()){
                    AbstractMap.SimpleEntry<String, String> kvPair = producer.next();
                    if(null != kvPair && null != kvPair.getKey()){
                        outKey.set(kvPair.getKey());
                        outValue.set(kvPair.getValue());
                        context.write(outKey, outValue);
                    }
                }
            }
        }

        /**
         * Check the key variable is initialized or not, if not, it's not ready to start map reduce job
         * @return true if input parameter is valid, else false
         */
       private boolean isValidMapReduceInputParameters() {
            return null != dataType;
        }

        @Override
        public void run(Context context) throws IOException, InterruptedException {
            setup(context);
            if (! isValidMapReduceInputParameters()) {
                context.getCounter(MapReducerFeatureExtractionWorker.RunningCount.INPUT_PARAMETER_ERROR).increment(1L);
                LOG.error(CcfConstants.LOG_PREFIX + "Input parameter not right!");
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

    private static class FeatureExtractionReducer extends Reducer<Text, Text, Text, Text> {
        private static Text outKey = new Text();
        private static Text outValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text value: values){
                String[] info = CcfUtils.getRecordInfo(value.toString(), CcfConstants
                        .MAP_KEY_INNER_KEY_VALUE_SEPARATOR, 2);
                if(null == info) continue;


            }
        }
    }
}
