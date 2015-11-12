package com.haifwu.prince;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;


/**
 * Created by haifwu on 2015/11/12.
 */
public class ReadWrite {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: read file and write file <in> <out>");
            System.exit(2);
        }

        JobConf job = new JobConf(new Configuration(), ReadWrite.class);
        FileSystem fs = FileSystem.get(job);
        FSDataInputStream inputStream = fs.open(new Path(otherArgs[0]));
        FSDataOutputStream outputStream = fs.create(new Path(otherArgs[1]));
        String lineRead, lineWrite;
        int pos = 0;
        BufferedReader reader = null;
        BufferedWriter writer = null;

        try{
            reader = new BufferedReader(new InputStreamReader(inputStream));
            writer = new BufferedWriter(new OutputStreamWriter(outputStream));

            while((lineRead = reader.readLine())!= null){
                lineWrite = "" + pos + "\t" + lineRead.length();
                pos += lineRead.length();
                writer.write(lineWrite);
                writer.newLine();
            }
        } finally {
            if(reader != null) reader.close();
            if(writer != null) writer.close();
        }
    }

}
