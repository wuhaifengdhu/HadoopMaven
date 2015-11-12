package com.haifwu.prince;

/**
 * Created by haifwu on 2015/11/12.
 */
public class Record {

    private String filePath;
    private String offset;
    private String length;


    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public Record(String offset, String length) {
        this.offset = offset;
        this.length = length;
    }

    public Record(String filePath, String offset, String length) {
        this.filePath = filePath;
        this.offset = offset;
        this.length = length;
    }

    public String getOffset() {
        return offset;
    }

    public void setOffset(String offset) {
        this.offset = offset;
    }

    public String getLength() {
        return length;
    }

    public void setLength(String length) {
        this.length = length;
    }

    @Override
    public String toString() {
        return filePath + "\t" + offset + "\t" + length;
    }
}
