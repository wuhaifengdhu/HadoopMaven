package com.rainyday.ccf.feature.container.extractable;

/**
 * Created by haifwu on 2016/11/1.
 */
public interface Extractable {
    /**
     *  Key for this feature
     * @return key
     */
    String getKey();

    /**
     *  Value for this feature
     * @return value
     */
    String getValue();
}
