package com.rainyday.ccf.feature.container.extractable;

/**
 * @author haifwu
 */
public interface Computable {
     void reset();
     void add(String line);
     void compute();
     String getComputeResult();
}
