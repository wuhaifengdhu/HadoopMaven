package com.rainyday.ccf.feature.container.data;

/**
 * Created by haifwu on 2016/11/4.
 */
public interface Buildable {
    /**
     *  Build an AbstractData, if records invalid, then return null
     *
     * @param line the training data record
     * @return Abstract Data after build
     */
    AbstractData build();
}
