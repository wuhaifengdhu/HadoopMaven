package com.rainyday.ccf.feature.container.extractable;

import com.rainyday.ccf.feature.container.data.AbstractData;
import com.rainyday.ccf.feature.util.CcfConstants;
import com.rainyday.ccf.feature.util.CcfUtils;

/**
 * @author haifwu
 */
public class CouponTendencyExtractable implements Extractable, Computable {
    private AbstractData record;

    private long validDiscountRateRecordsNum;
    private int recordTimes;
    private int clickTimes;
    private int collectionTimes;
    private int usedWithin15DaysTimes;
    private int usedOutOf15DaysTimes;
    private float averageDiscountRate;

    public CouponTendencyExtractable(AbstractData data) {
        this.record = data;
    }

    public CouponTendencyExtractable() {
    }

    /**
     * How many times this coupon being accessed by users.
     * 
     * @return total record time related to this coupon
     */
    public int getRecordTimes() {
        return 1;
    }

    /**
     * How many times this coupon being clicked.
     * 
     * @return clicked times
     */
    int getClickedTimes() {
        return record.isOnlineClickCouponType() ? 1 : 0;
    }

    /**
     * How many time this coupon being collected by users
     * 
     * @return collected times
     */
    int getCollectTimes() {
        return record.isOnlineCollectCouponType() ? 1 : 0;
    }

    /**
     * How many times this coupon being used within 15 days Here we focus on
     * offline use coupon, that's what we will predict
     * 
     * @return used within 15 days times
     */
    int getUsedWithin15DaysTimes() {
        return record.isCouponBeingUsedOfflineWithin15Days() ? 1 : 0;
    }

    /**
     * How many times this coupon being used out of 15 days Here we focus on
     * offline use coupon, that's what we will predict
     * 
     * @return
     */
    int getUsedOutOf15DaysTimes() {
        return record.isCouponBeingUsedOfflineOutOf15Days() ? 1 : 0;
    }

    /**
     * Get coupon's history average discount rate
     * 
     * @return average discount rate
     */
    float getAverageDiscountRate() {
        return record.getDiscountRate();
    }

    /**
     * TODO for coupon id is 'null', we should ignore when we statistic
     * 
     * @return coupon id
     */
    @Override
    public String getKey() {
        return FeatureType.COUPON_TENDENCY.toString();
    }

    @Override
    public String getValue() {
        if (CcfUtils.isNullValue(record.getCouponId())) {
            return CcfConstants.EMPTY_STRING;
        }
        StringBuilder builder = new StringBuilder(50);
        builder.append(record.getCouponId()).append(CcfConstants.MAP_KEY_INNER_KEY_VALUE_SEPARATOR)
                .append(getRecordTimes()).append(CcfConstants.COLUMN_SEPARATOR)
                .append(getClickedTimes()).append(CcfConstants.COLUMN_SEPARATOR)
                .append(getCollectTimes()).append(CcfConstants.COLUMN_SEPARATOR)
                .append(getUsedWithin15DaysTimes()).append(CcfConstants.COLUMN_SEPARATOR)
                .append(getUsedOutOf15DaysTimes()).append(CcfConstants.COLUMN_SEPARATOR)
                .append(getAverageDiscountRate());
        return builder.toString();
    }

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
