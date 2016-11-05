package com.rainyday.ccf.feature.mapreduce.impl;

import com.rainyday.ccf.feature.container.extractable.FeatureType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import javax.swing.text.TabExpander;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author haifwu
 */
public class FeatureWritable implements Writable {
    /**
     *  Feature type
     */
    private Text featureType;

    /**
     *  Feature Object serialize string
     */
    private Text featureObject;

    public FeatureWritable(Text type, Text body){
        super();
        this.featureType = type;
        this.featureObject = body;
    }

    public synchronized Text getFeatureType() {
        return featureType;
    }

    public synchronized void setFeatureType(Text featureType) {
        this.featureType = featureType;
    }

    public synchronized Text getFeatureObject() {
        return featureObject;
    }

    public synchronized void setFeatureObject(Text featureObject) {
        this.featureObject = featureObject;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.featureType.write(out);
        this.featureObject.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.featureType.readFields(in);
        this.featureObject.readFields(in);
    }
}
