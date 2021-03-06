package com.rainyday.ccf.feature.container.extractable;

import com.rainyday.ccf.feature.container.data.AbstractData;
import com.rainyday.ccf.feature.util.CcfConstants;
import com.rainyday.ccf.feature.util.CcfUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author haifwu
 */
public class UserMerchantExtractable implements Extractable, Computable{
    private static final Logger LOG = LoggerFactory.getLogger(UserMerchantExtractable.class);

    private AbstractData record;
    private int actionTimes;

    public UserMerchantExtractable(AbstractData data){
        this.record = data;
    }

    public int getActionTimes(){
        return 1;
    }


    @Override
    public void reset() {
        this.actionTimes = 0;
    }

    @Override
    public void add(String line) {
        this.actionTimes += CcfUtils.getIntValue(line);
    }

    @Override
    public String getComputeResult() {
        StringBuilder builder = new StringBuilder(20);
        builder.append(this.actionTimes);
        return builder.toString();
    }

    @Override
    public String getKey() {
        return FeatureType.USER_MERCHANT.toString();
    }

    @Override
    public String getValue() {
        StringBuilder builder = new StringBuilder(80);
        builder.append(record.getUserId()).append(CcfConstants.COLUMN_SEPARATOR)
                .append(record.getMerchantId()).append(CcfConstants.MAP_KEY_INNER_KEY_VALUE_SEPARATOR)
                .append(getActionTimes());
        return builder.toString();
    }
}
