/*
 * Copyright (c) 2015 PayPal Corporation. All rights reserved.
 *
 * Created on 2015-11-24
 */
package com.haifwu.prince;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author haifwu
 *
 */
public class ActivityIdEmptyCheck {
    
    private static final Logger LOG = LoggerFactory.getLogger(ActivityIdEmptyCheck.class);

    /**
     * @param args
     * @throws IOException 
     */
    public static void main(String[] args) throws IOException {
        // TODO Auto-generated method stub
        List<Record> testRecords = prepareRecords();
        readAndWrite(testRecords, "activityIdEmptyCheck.result");
    }

    /**
     * @return
     */
    private static List<Record> prepareRecords() {
        List<Record> records = new ArrayList<Record>();
        String filePath1 = "hdfs://horton/sys/pp_dm/dm_hdp_batch/kafka_data/RISK/BLOGGING/idiriskaccessserv/SC_BREFundingCheckpoint_BREFundingCheckpoint/archive/2015/06/02/00/SC_BREFundingCheckpoint_BREFundingCheckpoint_1433231816502_1_0_slc-a.dat";
        String filePath2 = "hdfs://horton/sys/pp_dm/dm_hdp_batch/kafka_data/RISK/BLOGGING/idiriskaccessserv/SC_BREFundingCheckpoint_BREFundingCheckpoint/archive/2015/06/02/00/SC_BREFundingCheckpoint_BREFundingCheckpoint_1433231816502_1_0_slc-a.dat";
        String filePath3 = "hdfs://horton/sys/pp_dm/dm_hdp_batch/kafka_data/RISK/BLOGGING/idiriskaccessserv/SC_BREFundingCheckpoint_BREFundingCheckpoint/archive/2015/06/02/00/SC_BREFundingCheckpoint_BREFundingCheckpoint_1433230314108_1_0_slc-a.dat";
        Record record1 = new Record(filePath1, 246475931L, 35016);
        records.add(record1);
        Record record2 = new Record(filePath2, 246394256L, 35013);
        records.add(record2);
        Record record3 = new Record(filePath3, 1607303394, 36024);
        records.add(record3);
        return records;
    }
    
    /**
     * Read one line according to the file path and offset in record
     * @param record, contain the file path and offset attribute needed to read
     * @param outputFile, the file to store the content read
     * @throws IOException
     */
    private static void readAndWrite(List<Record> records, String outputFile) throws IOException {
        BufferedReader reader = null;
        BufferedWriter writer = null;

        try{
            FileSystem fs = FileSystem.get(new Configuration());

            FSDataOutputStream outputStream = fs.append(new Path(outputFile));
            writer = new BufferedWriter(new OutputStreamWriter(outputStream));

            for(Record record: records){
                LOG.info("Random read on file {} start", record.getFilePath());
                // Read from file
                FSDataInputStream inputStream = fs.open(new Path(record.getFilePath()));
                inputStream.seek(record.getOffset());
                reader = new BufferedReader(new InputStreamReader(inputStream));
                String lineRead = reader.readLine();

                // Write the result to file
                writer.write(lineRead);
                writer.newLine();
                LOG.info("Random read on file {} finished", record.getFilePath());
            }
            
        }
        catch (IOException e){
            throw new RuntimeException(e);
        }
        finally {
            if(reader != null) reader.close();
            if(writer != null) writer.close();
        }
    }
}
