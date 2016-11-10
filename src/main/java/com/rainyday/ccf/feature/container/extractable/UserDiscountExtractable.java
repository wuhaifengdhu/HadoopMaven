package com.rainyday.ccf.feature.container.extractable;

import com.rainyday.ccf.feature.container.data.AbstractData;
import com.rainyday.ccf.feature.util.CcfConstants;
import com.rainyday.ccf.feature.util.CcfUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author haifwu
 */
public class UserDiscountExtractable implements Extractable, Computable{
    private static final Logger LOG = LoggerFactory.getLogger(UserDiscountExtractable.class);

    private AbstractData record;
    private float averageDiscountRate;
    private int validCount;

    public UserDiscountExtractable(AbstractData data){
        this.record = data;
    }

    public float getCouponDiscount(){
        if(record.isOfflineCouponBuy()){
            return record.getDiscountRate();
        }
        return 0;
    }


    @Override
    public void reset() {
        this.averageDiscountRate = 0;
        this.validCount = 0;
    }

    @Override
    public void add(String line) {
        float value = CcfUtils.getFloatValue(line);
        if(value > 0){
            this.validCount += 1;
            this.averageDiscountRate = (this.averageDiscountRate * (this.validCount -1) + value) / this.validCount;
        }
    }

    @Override
    public String getComputeResult() {
        StringBuilder builder = new StringBuilder(20);
        builder.append(this.averageDiscountRate);
        return builder.toString();
    }

    @Override
    public String getKey() {
        return FeatureType.USER_DISCOUNT.toString();
    }

    @Override
    public String getValue() {
        StringBuilder builder = new StringBuilder(80);
        builder.append(record.getUserId()).append(CcfConstants.MAP_KEY_INNER_KEY_VALUE_SEPARATOR)
                .append(getCouponDiscount());
        return builder.toString();
    }
}
