package com.rainyday.ccf.feature.mapreduce.impl;

import com.rainyday.ccf.feature.container.extractable.FeatureType;
import com.rainyday.ccf.feature.util.CcfConstants;
import com.rainyday.ccf.feature.util.CcfUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

/**
 * @author haifwu
 */
public class FeatureOutputFormat<K,V> extends FileOutputFormat<K, V> {
    private static final Logger LOG = LoggerFactory.getLogger(FeatureOutputFormat.class);

    @Override
    public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        return new FeatureWriter<K, V>(job.getConfiguration());
    }

    protected static class FeatureWriter<K, V> extends RecordWriter<K, V> {

        private static final String utf8 = "UTF-8";
        private static final byte[] newline;
//        private static byte[] keyValueSeparator;
        private final ArrayList<FSDataOutputStream> outputStreams = new ArrayList<FSDataOutputStream>(FeatureType
                .values().length + 1);
        static {
            try {
                newline = "\n".getBytes(utf8);
            } catch (UnsupportedEncodingException uee) {
                throw new IllegalArgumentException("can't find " + utf8 + " encoding");
            }
        }

        public FeatureWriter(Configuration conf) throws IOException {
            FileSystem fs = FileSystem.newInstance(conf);
            String outputPath = conf.get(org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.OUTDIR, CcfConstants
                    .EMPTY_STRING);
//            keyValueSeparator = conf.get("mapreduce.output.textoutputformat.separator", "\t").getBytes();
            for(FeatureType type: FeatureType.values()){
                outputStreams.add(type.ordinal(), fs.create(new Path(outputPath, type.toString())));
            }
            outputStreams.add(FeatureType.values().length, fs.create(new Path(outputPath, "UNKNOWN")));
        }

        @Override
        public void write(K key, V value) throws IOException {
            boolean nullKey = CcfUtils.isNullValue(key)|| key instanceof NullWritable || CcfUtils.isNullValue(key
                    .toString());
            boolean nullValue = CcfUtils.isNullValue(value) || value instanceof NullWritable || CcfUtils.isNullValue
                    (value.toString());
            // Either null key or null value regarded as invalid information
            if (nullKey || nullValue) {
                return;
            }

            FSDataOutputStream out;
            try{
                FeatureType type = FeatureType.valueOf(key.toString());
                out = outputStreams.get(type.ordinal());
            } catch (IllegalArgumentException ignore){
                LOG.error("Unidentified key in FeatureWriter, use UNKOWN output");
                out = outputStreams.get(FeatureType.values().length);
            }
//            Comment following line because we don't need output key
//            if (!nullKey) {
//                writeObject(out, key);
//            }
//            if (!(nullKey || nullValue)) {
//                out.write(keyValueSeparator);
//            }
            writeObject(out, value);
            out.write(newline);
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            for (FSDataOutputStream out: outputStreams) {
                out.close();
            }
        }

        /**
         * Write the object to the byte stream, handling Text as a special
         * case.
         * @param o the object to print
         * @throws IOException if the write throws, we pass it on
         */
        private void writeObject(FSDataOutputStream out, Object o) throws IOException {
            if (o instanceof Text) {
                Text to = (Text) o;
                out.write(to.getBytes(), 0, to.getLength());
            } else {
                out.write(o.toString().getBytes(utf8));
            }
        }
    }
}
