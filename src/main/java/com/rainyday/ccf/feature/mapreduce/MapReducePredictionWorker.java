package com.rainyday.ccf.feature.mapreduce;

import com.rainyday.ccf.feature.model.Model;
import com.rainyday.ccf.feature.model.NNModel;
import com.rainyday.ccf.feature.util.CcfConstants;
import com.rainyday.ccf.feature.util.CcfUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
public class MapReducePredictionWorker {
    private static final Logger LOG = LoggerFactory.getLogger(MapReducePredictionWorker.class);

    enum Running_Record{
        INPUT_LINE_NULL, MODEL_RESUlT_NULL
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        // check input parameters
        Configuration conf = new Configuration();
        new GenericOptionsParser(conf, args);
        Path inputPath = new Path(conf.get(CcfConstants.INPUT_PATH));
        Path outputPath = new Path(conf.get(CcfConstants.OUTPUT_PATH));
        if (!inputOutputPathSetup(conf, inputPath, outputPath)) {
            LOG.error("Check input path output path failed!");
            return;
        }

        // Setting job information
        Job job = Job.getInstance(conf, "MapReducePredictionWorker");
        job.setJarByClass(MapReducePredictionWorker.class);
        job.setMapperClass(MapReducePredictionWorker.PredictionMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileInputFormat.setInputDirRecursive(job, true);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, outputPath);
        FileOutputFormat.setCompressOutput(job, false);

        int code = job.waitForCompletion(true) ? 0 : 1;
        if (code != 0) {
            LOG.error(CcfConstants.LOG_PREFIX + "MapReducePredictionWorker job running failed!");
            return;
        }
        LOG.info("MapReducePredictionWorker Job running succeeded! Total time: {}s.",
                (System.currentTimeMillis() - startTime) / 1000);
    }

    private static boolean inputOutputPathSetup(Configuration conf, Path inputPath, Path outputPath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        if (null == inputPath || null == outputPath) {
            LOG.error("Invalid input output path <" + CcfUtils.getNoNullString(inputPath) + ","
                    + CcfUtils.getNoNullString(outputPath) + ">");
            return false;
        }
        if (!fs.exists(inputPath)) {
            LOG.error("Input path not exist! inputPath = " + CcfUtils.getNoNullString(inputPath));
            return false;
        }
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        return true;
    }

    private static class PredictionMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        private static Text outValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Model model = new NNModel();
            String line = value.toString();
            if(CcfUtils.isNullValue(line)) {
                LOG.error("Input line empty in PredictionMapper, line:" + CcfUtils.getNoNullString(line));
                context.getCounter(Running_Record.INPUT_LINE_NULL).increment(1L);
            }
            String modelResult = model.getModelResult(line);
            if(CcfUtils.isNullValue(model)){
                LOG.error("Model result generated null, line:" + CcfUtils.getNoNullString(line));
                context.getCounter(Running_Record.MODEL_RESUlT_NULL).increment(1L);
            }
            outValue.set(modelResult);
            context.write(NullWritable.get(), outValue);
        }
    }
}
