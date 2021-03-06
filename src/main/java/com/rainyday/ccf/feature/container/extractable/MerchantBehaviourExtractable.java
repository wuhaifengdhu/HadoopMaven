package com.rainyday.ccf.feature.container.extractable;

import com.rainyday.ccf.feature.container.data.AbstractData;
import com.rainyday.ccf.feature.util.CcfConstants;
import com.rainyday.ccf.feature.util.CcfUtils;

/**
 * Created by haifwu on 2016/11/1.
 */
public class MerchantBehaviourExtractable implements Extractable, Computable{
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
        builder.append(record.getMerchantId()).append(CcfConstants.MAP_KEY_INNER_KEY_VALUE_SEPARATOR)
                .append(getRecordTimes()).append(CcfConstants.COLUMN_SEPARATOR)
                .append(getClickedTimes()).append(CcfConstants.COLUMN_SEPARATOR)
                .append(getCollectTimes()).append(CcfConstants.COLUMN_SEPARATOR)
                .append(getUsedWithin15DaysTimes()).append(CcfConstants.COLUMN_SEPARATOR)
                .append(getUsedOutOf15DaysTimes()).append(CcfConstants.COLUMN_SEPARATOR)
                .append(getAverageDiscountRate());
        return builder.toString();
    }
    
    private long validDiscountRateRecordsNum;
    
    private int recordTimes;
    private int clickTimes;
    private int collectionTimes;
    private int usedWithin15DaysTimes;
    private int usedOutOf15DaysTimes;
    private float averageDiscountRate;

    @Override
    public void reset() {
     // for calculate
        this.validDiscountRateRecordsNum = 0;

        // Valid field
        this.recordTimes = 0;
        this.clickTimes = 0;
        this.collectionTimes = 0;
        this.usedWithin15DaysTimes = 0;
        this.usedOutOf15DaysTimes = 0;
        this.averageDiscountRate = 0;
    }

    @Override
    public void add(String line) {
        String[] info = CcfUtils.getRecordInfo(line, CcfConstants.COLUMN_SEPARATOR, 6);
        if(null != info){
            this.recordTimes += CcfUtils.getIntValue(info[0]);
            this.clickTimes += CcfUtils.getIntValue(info[1]);
            this.collectionTimes += CcfUtils.getIntValue(info[2]);
            this.usedWithin15DaysTimes += CcfUtils.getIntValue(info[3]);
            this.usedOutOf15DaysTimes += CcfUtils.getIntValue(info[4]);
            float rate = CcfUtils.getFloatValue(info[5]);
            if(rate >= 0){
                this.validDiscountRateRecordsNum += 1;
                this.averageDiscountRate = (this.averageDiscountRate * (this.validDiscountRateRecordsNum - 1) + rate)/ this.validDiscountRateRecordsNum;
            }
        }
    }

    @Override
    public String getComputeResult() {
        StringBuilder builder = new StringBuilder(50);
        builder.append(this.recordTimes).append(CcfConstants.COLUMN_SEPARATOR)
                .append(this.clickTimes).append(CcfConstants.COLUMN_SEPARATOR)
                .append(this.collectionTimes).append(CcfConstants.COLUMN_SEPARATOR)
                .append(this.usedWithin15DaysTimes).append(CcfConstants.COLUMN_SEPARATOR)
                .append(this.usedOutOf15DaysTimes).append(CcfConstants.COLUMN_SEPARATOR)
                .append(this.averageDiscountRate);
        return builder.toString();
    }
}
