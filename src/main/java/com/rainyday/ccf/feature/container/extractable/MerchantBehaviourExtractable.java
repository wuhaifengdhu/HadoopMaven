package com.rainyday.ccf.feature.container.extractable;

import com.rainyday.ccf.feature.container.data.AbstractData;
import com.rainyday.ccf.feature.util.CcfConstants;

/**
 * Created by haifwu on 2016/11/1.
 */
public class MerchantBehaviourExtractable implements Extractable{
    private AbstractData record;

    public MerchantBehaviourExtractable(AbstractData data){
        this.record = data;
    }

    /**
     *  How many times this merchant being accessed by users.
     * @return total record time related to this merchant
     */
    int getRecordTimes(){
        return 1;
    }

    /**
     *  How many times this merchant being clicked.
     *  Only online type training data has this field
     * @return clicked times
     */
    int getClickedTimes(){
        return record.isOnlineClickCouponType() ? 1 : 0;
    }

    /**
     *  How many time this merchant being collected by users
     *  Only online type training data has this field
     * @return collected times
     */
    int getCollectTimes(){
        return record.isOnlineCollectCouponType() ? 1 : 0;
    }

    /**
     *  How many times this merchant being used within 15 days
     *  Here we mainly focus on offline coupon use, which we predict for
     * @return used within 15 days times
     */
    int getUsedWithin15DaysTimes(){
        return record.isCouponBeingUsedOfflineWithin15Days() ? 1 : 0;
    }

    /**
     *  How many times this merchant being used out of 15 days
     *  Here we mainly focus on offline coupon use, which we predict for
     * @return
     */
    int getUsedOutOf15DaysTimes(){
        return record.isCouponBeingUsedOfflineOutOf15Days() ? 1 : 0;
    }

    /**
     *  Get merchant's history average discount rate
     * @return average discount rate
     */
    float getAverageDiscountRate(){
        return record.getDiscountRate();
    }

    @Override
    public String getKey() {
        return FeatureType.MERCHANT_BEHAVIOUR.toString();
    }

    @Override
    public String getValue() {
        StringBuilder builder = new StringBuilder(50);
        builder.append(record.getMerchantId()).append(CcfConstants.COLUMN_SEPARATOR)
                .append(getRecordTimes()).append(CcfConstants.COLUMN_SEPARATOR)
                .append(getClickedTimes()).append(CcfConstants.COLUMN_SEPARATOR)
                .append(getCollectTimes()).append(CcfConstants.COLUMN_SEPARATOR)
                .append(getUsedWithin15DaysTimes()).append(CcfConstants.COLUMN_SEPARATOR)
                .append(getUsedOutOf15DaysTimes()).append(CcfConstants.COLUMN_SEPARATOR)
                .append(getAverageDiscountRate());
        return builder.toString();
    }
}
