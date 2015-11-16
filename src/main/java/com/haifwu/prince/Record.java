package com.haifwu.prince;

/**
 * Created by haifwu on 2015/11/12.
 */
public class Record {

    private String filePath;
    private long offset;
    private int length;


    public Record(String filePath, long offset, int length) {
        this.filePath = filePath;
        this.offset = offset;
        this.length = length;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    @Override
    public String toString() {
        return filePath + "\t" + offset + "\t" + length;
    }
}
