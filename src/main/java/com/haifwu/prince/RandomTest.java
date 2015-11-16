package com.haifwu.prince;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;

/**
 * Created by haifwu on 2015/11/12.
 */
public class RandomTest {
    private static final Logger LOG = LoggerFactory.getLogger(RandomTest.class);
    private static int failureTimes = 0;
    private static final int MAX_RANDOM_READ_TIMES = 20000;

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: random read test <directory> <indexStoreFile> <outputFile");
            System.exit(1);
        }
        preProcessArgument(otherArgs[1], otherArgs[2]);

        DirectoryIndex directoryIndex = new DirectoryIndex();
        List<Record> indexList = directoryIndex.generateWholeIndex(otherArgs[0], otherArgs[1]);
        if(indexList.isEmpty()){
            System.err.println("Usage: random read test <directory> <indexStoreFile> <outputFile");
            System.exit(2);
        }

        long startTime = System.currentTimeMillis();
        LOG.info("Random read test start! timestamp: {}", startTime);
        for(int count = 0; count < MAX_RANDOM_READ_TIMES; count++){
            Record record = getRandomRecord(indexList);
            readAndWrite(record, otherArgs[2]);
            LOG.info("{} records done!", count);
        }
        long endTime = System.currentTimeMillis();
        LOG.info("Random read test finished! timestamp: {}", endTime);
        LOG.info("Total failure times: {}", failureTimes);
        LOG.info("Total used time: {}ms, each read and write used time {}ms", (endTime - startTime), (endTime - startTime)/ MAX_RANDOM_READ_TIMES);
    }

    /**
     * delete index store file and output file if exist before, and create empty output file to store the output result
     * @param indexStoreFile, the file name to store the generated index records
     * @param outputFile, the output file to store the random read result
     */
    private static void preProcessArgument(String indexStoreFile, String outputFile) {
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            fs.deleteOnExit(new Path(indexStoreFile));
            fs.create(new Path(outputFile), true);
            fs.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * Read one line according to the file path and offset in record
     * @param record, contain the file path and offset attribute needed to read
     * @param outputFile, the file to store the content read
     * @throws IOException
     */
    private static void readAndWrite(Record record, String outputFile) throws IOException {
        LOG.info("Random read on file {} start", record.getFilePath());

        BufferedReader reader = null;
        BufferedWriter writer = null;

        try{
            FileSystem fs = FileSystem.get(new Configuration());

            FSDataOutputStream outputStream = fs.append(new Path(outputFile));
            writer = new BufferedWriter(new OutputStreamWriter(outputStream));

            FSDataInputStream inputStream = fs.open(new Path(record.getFilePath()));
            inputStream.seek(record.getOffset());
            reader = new BufferedReader(new InputStreamReader(inputStream));
            String lineRead = reader.readLine();

            writer.write(lineRead);
            writer.newLine();
        }
        catch (IOException e){
            failureTimes += 1;
            throw new RuntimeException(e);
        }
        finally {
            if(reader != null) reader.close();
            if(writer != null) writer.close();
        }

        LOG.info("Random read on file {} finished", record.getFilePath());
    }

    /**
     * Use Math random method to make random choose record within list
     * @param indexList, the index list to choose record from
     * @return
     */
    private static Record getRandomRecord(List<Record> indexList) {
        int index = (int)(Math.random() * (indexList.size() - 1));
        return indexList.get(index);
    }
}
