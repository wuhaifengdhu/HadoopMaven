package com.haifwu.prince;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by haifwu on 2015/11/12.
 */
public class DirectoryIndex {
    private static final Logger LOG = LoggerFactory.getLogger(DirectoryIndex.class);

    private List<Record> indexList = new ArrayList<Record>();

    /**
     * Generate whole index under a directory, and store it into file
     * @param directory, the directory which to make index
     * @param indexStoreFile, the file to store index
     */
    public List<Record> generateWholeIndex(String directory, String indexStoreFile){
        List<String> dataFiles = listFilesUnderDirectory(directory);
        for(String file : dataFiles){
            List<Record> fileIndexes = null;
            try {
                fileIndexes = generateFileIndexes(file);
            } catch (IOException e) {
                LOG.debug("Failed when generate index for {}", file);
            }
            indexList.addAll(fileIndexes);
        }
        try {
            storeIndexToFile(indexStoreFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return indexList;
    }

    /**
     * Store the generated index into a file
     * @param indexStoreFile, the file to store indexes
     */
    private void storeIndexToFile(String indexStoreFile) throws IOException {
        LOG.info("start store indexes to file", indexStoreFile);
        BufferedWriter writer = null;
        try{
            FileSystem fs = FileSystem.get(new Configuration());
            FSDataOutputStream outputStream = fs.create(new Path(indexStoreFile));
            writer = new BufferedWriter(new OutputStreamWriter(outputStream));

            for(Record record: indexList){
                writer.write(record.toString());
                writer.newLine();
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if(writer != null) writer.close();
        }
        LOG.info("finished store indexes to file", indexStoreFile);
    }

    /**
     * Generate index of a file, each line one record
     * @param file, the file to generate index
     * @return file indexes in one file
     */
    private List<Record> generateFileIndexes(String file) throws IOException {
        LOG.info("Generate index for {}", file);
        List<Record> fileIndexes = new ArrayList<Record>();
        BufferedReader reader = null;

        try{
            FileSystem fs = FileSystem.get(new Configuration());
            FSDataInputStream inputStream = fs.open(new Path(file));
            String lineRead;
            long pos = 0;

            reader = new BufferedReader(new InputStreamReader(inputStream));

            while((lineRead = reader.readLine())!= null){
                fileIndexes.add(new Record(file, pos, lineRead.length()));
                pos += lineRead.length();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if(reader != null) reader.close();
        }
        LOG.info("finished Generate index for {}", file);
        return fileIndexes;
    }

    /**
     * List all files under one directory
     * @param directory, the directory to list
     * @return array of path under directory
     */
    private List<String> listFilesUnderDirectory(String directory) {
        List<String> files = new ArrayList<String>();
        try{
            FileSystem fs = FileSystem.get(new Configuration());
            RemoteIterator<LocatedFileStatus> fileStatusRemoteIterator = fs.listFiles(new Path(directory), true);

            while (fileStatusRemoteIterator.hasNext()){
                LocatedFileStatus fileStatus = fileStatusRemoteIterator.next();
                files.add(fileStatus.getPath().toString());
            }
        } catch (Exception e){
            throw new RuntimeException(e);
        }
        return files;
    }


}
