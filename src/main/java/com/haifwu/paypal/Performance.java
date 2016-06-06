package com.haifwu.paypal;

import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
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

import com.google.common.base.Stopwatch;
import com.haifwu.util.Common;
import com.haifwu.util.Constants;


public class Performance {
    private static final Logger LOG = LoggerFactory.getLogger(Performance.class);
    
    enum PerformanceErrorConter{
        DATE_PARSE_EXCEPTION, VALID_RECORD, REDUCE_INPUT_SPLIT_ERROR
    }
    
    public static class PerformanceMapper extends Mapper<LongWritable, Text, Text, Text>{
        private static Text outKey = new Text();
        private static Text outValue = new Text();
        
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            outKey.set(fileName);
            String line = value.toString();
            if(line.contains("End to call HBase.get")){
                String usedTime = Common.getLastStringBetween(line, "with", "milliseconds");
                String timeStamp = line.substring(0, 19);
                LOG.info("timeStamp: " + timeStamp);
                try {
                    String pit = String.valueOf(Common.convertDateToPIT(timeStamp, "yyyy-MM-dd HH:mm:SS"));
                    outValue.set(pit + Constants.PIPESTR + usedTime);
                    context.getCounter(PerformanceErrorConter.VALID_RECORD).increment(1L);
                    context.write(outKey, outValue);
                } catch (ParseException e) {
                    LOG.error("Parse timeStamp error, timeStamp:" + timeStamp);
                    context.getCounter(PerformanceErrorConter.DATE_PARSE_EXCEPTION).increment(1L);
                }
            }
        }
    }

    public static class  PerformanceReducer extends Reducer<Text, Text, Text, Text>{

        private static Text outValue = new Text();
        
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            long start = Long.MAX_VALUE;
            long end = Long.MIN_VALUE;
            long count = 0;
            double sla = 0;
            
            for(Text value: values){
                String[] info = value.toString().split("\\|");
                if(info.length == 2 && info[0] != null && info[1] != null){
                    long pit = Long.valueOf(info[0]).longValue();
                    start = Math.min(start, pit);
                    end = Math.max(end, pit);
                    sla = (count * 1.0 / (count + 1)) * sla + Long.valueOf(info[1]) * 1.0 / (count + 1);
                    count ++;
                } else {
                    LOG.info("split error for {}", info.toString());
                    context.getCounter(PerformanceErrorConter.REDUCE_INPUT_SPLIT_ERROR).increment(1L);
                }
            }
            long tps = end - start > 0 ? count * 1000 / (end - start): count ;
            outValue.set(tps + Constants.COLUMN_PIPESTR + sla + Constants.COLUMN_PIPESTR + count);
            context.write(key, outValue);
        }
    }

    /**
     * @param args
     * @throws IOException 
     * @throws InterruptedException 
     * @throws ClassNotFoundException 
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Stopwatch sw = new Stopwatch().start();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Performance test on vbase log");
        new GenericOptionsParser(conf, args);
        
        Path inputPath = new Path(conf.get(Constants.INPUT_PATH));
        Path outputPath = new Path(conf.get(Constants.OUTPUT_PATH));
        FileSystem fs = FileSystem.get(conf);
        if(! fs.exists(inputPath)){
            LOG.error("The input path {} is not exist!", inputPath);
            System.exit(1);
        }
        if(fs.exists(outputPath)){
            fs.delete(outputPath, true);
        }
        Path[] paths = FileUtil.stat2Paths(fs.listStatus(inputPath, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                if(path.toString().contains("_SUCCESS")){
                    return false;
                }
                return true;
            }
        }));
        
        job.setJarByClass(Performance.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setMapperClass(PerformanceMapper.class);
        job.setReducerClass(PerformanceReducer.class);
       
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(job, paths);
        FileOutputFormat.setOutputPath(job, outputPath);
        
        int status = job.waitForCompletion(true) ? 0: 1;
        LOG.info("Total job running for Time Taken: " + sw.stop().elapsedMillis() + "ms");
        System.exit(status);
    }
}
