package com.haifwu.prince;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class TestHTableGetNoExsitValue {

    private static final Logger LOG = LoggerFactory.getLogger(TestHTableGetNoExsitValue.class);

    private HTable htable;

    private void setup(){
        Configuration conf = HBaseConfiguration.create();
        try {
            htable = new HTable(conf, "SC_IRAS_BREFCP_ACTIVITY_ID_INDEX");
        } catch (IOException e) {
            LOG.error("IOException happened when create htable!");
        }
    }

    public TestHTableGetNoExsitValue(){
        setup();
    }

    public Result getResult(String rawKey){
        if(htable!= null ){
            Get query = new Get(rawKey.getBytes());
            try {
                return htable.get(query);
            } catch (IOException e) {
                LOG.error("IOException happend when read {} in hbase!", rawKey);
                return null;
            }
        } else {
            LOG.error("The HTable is not initialized!");
            return null;
        }
    }

    public void testFunction(){
        LOG.info("Test on exsit key!");
        String existKey = "100005315801267319660972321853974037968";
        Result result = getResult(existKey);
        print(result);
        
        LOG.info("Test on none exsit key!");
        String noExistKey = "imNotExist";
        result = getResult(noExistKey);
        print(result);
        
        if(htable != null){
            try {
                htable.close();
            } catch (IOException e) {
                LOG.info("Exception happended when close table!");
            }
        }
    }

    private void print(Result result) {
        LOG.info("Start print result!");
        if(result == null){
            LOG.info("result is null");
        } else {
            boolean exist = result.getExists();
            LOG.info("Exist result: {}",( exist? "True" : "False"));
            boolean isEmpty = result.isEmpty();
            LOG.info("Exist result: {}",( isEmpty ? "True" : "False"));
            String key = result.getRow().toString();
            LOG.info("getRow is {}", key);
            Cell cell = result.current();
            String qualifier = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
            LOG.info("qualifier is {}", qualifier);
            String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            LOG.info("value is {}", value);
        }
        LOG.info("Finished print result!"); 
    }

    public static void main(String[] args){
        TestHTableGetNoExsitValue test = new TestHTableGetNoExsitValue();
        test.testFunction();
    }

}
