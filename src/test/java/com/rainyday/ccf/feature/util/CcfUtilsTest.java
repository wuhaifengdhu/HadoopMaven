package com.rainyday.ccf.feature.util;

/**
 * @author haifwu
 */
public class CcfUtilsTest {
    public static void main(String[] args) throws Exception {
        String line = "1|0|0|0|0|0.0";
        String[] info = CcfUtils.getRecordInfo(line, CcfConstants.COLUMN_SEPARATOR, 6);
        if(null != info){
            for(String value: info){
                System.out.println(value);
            }
        } else {
            System.out.println("info is null");
        }
    }
}
