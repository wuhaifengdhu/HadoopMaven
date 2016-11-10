package com.rainyday.ccf.feature.container.extractable;

import com.rainyday.ccf.feature.container.data.AbstractData;
import com.rainyday.ccf.feature.util.CcfConstants;
import com.rainyday.ccf.feature.util.CcfUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author haifwu
 */
public class UserDistanceExtractable implements Extractable, Computable{
    private static final Logger LOG = LoggerFactory.getLogger(UserDistanceExtractable.class);

    private AbstractData record;
    private float averageDistance;
    private int validCount;

    public UserDistanceExtractable(AbstractData data){
        this.record = data;
    }

    public float getDistance(){
        if(record.isOfflineCouponBuy()){
            return record.getDistance();
        }
        return 0;
    }


    @Override
    public void reset() {
        this.averageDistance = 0;
        this.validCount = 0;
    }

    @Override
    public void add(String line) {
        float value = CcfUtils.getFloatValue(line);
        if(value > 0){
            this.validCount += 1;
            this.averageDistance = (this.averageDistance * (this.validCount -1) + value) / this.validCount;
        }
    }

    @Override
    public String getComputeResult() {
        StringBuilder builder = new StringBuilder(20);
        builder.append(this.averageDistance);
        return builder.toString();
    }

    @Override
    public String getKey() {
        return FeatureType.USER_DISTANCE.toString();
    }

    @Override
    public String getValue() {
        StringBuilder builder = new StringBuilder(80);
        builder.append(record.getUserId()).append(CcfConstants.MAP_KEY_INNER_KEY_VALUE_SEPARATOR)
                .append(getDistance());
        return builder.toString();
    }
}
