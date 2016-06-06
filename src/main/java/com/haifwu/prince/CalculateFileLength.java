/*
 * Copyright (c) 2015 PayPal Corporation. All rights reserved.
 *
 * Created on 2015-11-30
 */
package com.haifwu.prince;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.haifwu.util.Constants;


/**
 * @author haifwu
 *
 */
public class CalculateFileLength {
    
    private static final Logger LOG = LoggerFactory.getLogger(CalculateFileLength.class);
    
    enum BadRecordCounter{
        LESS_THAN_40_BYTE
    }
    
    public static class LineLengthCalculateMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            Text outKey = new Text();
            IntWritable outValue = new IntWritable();
            outValue.set(1);
            if(value.toString().length() < 40){
                context.getCounter(BadRecordCounter.LESS_THAN_40_BYTE).increment(1L);
                outKey.set(value);
                context.write(outKey, outValue);
            }
        }
    }
    
    public static class  LineLengthCalculateReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
                InterruptedException {
            int sum = 0;
            for(IntWritable value : values){
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
        
    }

    /**
     * @param args
     * @throws IOException 
     * @throws InterruptedException 
     * @throws ClassNotFoundException 
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.setInt("mapred.map.tasks", 100);
        Job job = Job.getInstance(conf, "Haifwu to calculate the lines less than 40 bytes in generated files");
        new GenericOptionsParser(conf, args);
        
        Path inputPath = new Path(conf.get(Constants.INPUT_PATH));
        Path outputPath = new Path(conf.get(Constants.OUTPUT_PATH));
        FileSystem fs = FileSystem.get(conf);
        if(! fs.exists(inputPath)){
            LOG.error("The input path {} is not exist!", inputPath);
            return;
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
        
        job.setJarByClass(CalculateFileLength.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setMapperClass(LineLengthCalculateMapper.class);
        job.setReducerClass(LineLengthCalculateReducer.class);
        job.setCombinerClass(LineLengthCalculateReducer.class);
       
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(job, paths);
        FileOutputFormat.setOutputPath(job, outputPath);
        
        System.exit(job.waitForCompletion(true) ? 0: 1);
    }

}
