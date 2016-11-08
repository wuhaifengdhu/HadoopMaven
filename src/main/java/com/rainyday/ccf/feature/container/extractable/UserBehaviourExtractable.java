package com.rainyday.ccf.feature.container.extractable;

import com.rainyday.ccf.feature.container.data.AbstractData;
import com.rainyday.ccf.feature.util.CcfConstants;
import com.rainyday.ccf.feature.util.CcfUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by haifwu on 2016/11/1.
 */
public class UserBehaviourExtractable implements Extractable, Computable {
    private static final Logger LOG = LoggerFactory.getLogger(UserBehaviourExtractable.class);

    private AbstractData record;
    // Calculate helper
    private int validDistanceAccountNum;
    private int validDiscountRateNum;
    //Field
    private int offlineDirectBuyWithoutCouponTimes;
    private int offlineDirectBuyHaveCouponTimes;
    private int offlineCouponBuyWithin15DaysTimes;
    private int offlineCouponBuyOut15DaysTimes;
    private float offlineCouponBuyAverageDistance;
    private float offlineCouponBuyWithin15DaysAverageDiscount;
    private int offlineCouponBuyWithin15DaysIsFixedDiscountTimes;
    private int onlineClickCouponTimes;
    private int onlineCollectCouponTimes;
    private int onlineDirectBuyWithoutCouponTimes;
    private int onlineDirectBuyHaveCouponTimes;
    private int onlineCouponBuyWithin15DaysTimes;
    private int onlineCouponBuyOutOf15DaysTimes;

    public UserBehaviourExtractable(AbstractData data) {
        this.record = data;
    }

    /**
     * User direct buy things offline, and he/she doesn't have coupon
     *
     * @return directly buy without coupon in hand times
     */
    public int getOfflineDirectBuyWithoutCouponTimes() {
        return record.isOfflineDirectBuyWithoutCoupon() ? 1 : 0;
    }

    /**
     * User direct buy things offline, through he/she has coupon ticket
     *
     * @return directly buy with coupon in hand times
     */
    public int getOfflineDirectBuyHaveCouponTimes() {
        return record.isOfflineDirectBuyHaveCoupon() ? 1 : 0;
    }

    /**
     * User buy offline used coupon in 15 days from get the coupon
     *
     * @return buy offline within 15 days times
     */
    public int getOfflineCouponBuyWithin15DaysTimes() {
        return record.isOfflineCouponBuyWithin15Days() ? 1 : 0;
    }

    /**
     * User buy offline used coupon out of 15 days from get the coupon
     *
     * @return buy offline out of 15 days times
     */
    public int getOfflineCouponBuyOut15DaysTimes() {
        return record.isOfflineCouponBuyOutOf15Days() ? 1 : 0;
    }

    /**
     * Calculate the average distance the user willing to buy offline use coupon
     * TODO we should calculate average value by none null value
     *
     * @return average distance the user willing to buy
     */
    public int getOfflineCouponBuyAverageDistance() {
        return record.isOfflineCouponBuy() ? record.getDistance() : 0;
    }

    /**
     * User offline buy use coupon with 15 days' average coupon
     *
     * @return offline user buy with coupon average discount
     */
    public float getOfflineCouponBuyWithin15DaysAverageDiscount() {
        return record.isOfflineCouponBuyWithin15Days() ? record.getDiscountRate() : 0;
    }

    /**
     * User offline buy use fixed type coupon times
     *
     * @return offline coupon buy is fixed discount times
     */
    public int getOfflineCouponBuyWithin15DaysIsFixedDiscountTimes() {
        return record.isOfflineCouponBuyWithin15Days() && record.isFixedDiscountRate() ? 1 : 0;
    }

    /**
     * User click the coupon but not collect or use it.
     *
     * @return online click coupon times
     */
    public int getOnlineClickCouponTimes() {
        return record.isOnlineClickCouponType() ? 1 : 0;
    }

    /**
     * User collect coupon but not use it.
     *
     * @return online collect coupon times
     */
    public int getOnlineCollectCouponTimes() {
        return record.isOnlineCollectCouponType() ? 1 : 0;
    }

    /**
     * User direct buy things online, and he/she doesn't have coupon
     *
     * @return directly buy without coupon in hand times
     */
    public int getOnlineDirectBuyWithoutCouponTimes() {
        return record.isOnlineDirectBuyWithoutCoupon() ? 1 : 0;
    }

    /**
     * User direct buy things online, through he/she has coupon ticket
     *
     * @return directly buy with coupon in hand times
     */
    public int getOnlineDirectBuyHaveCouponTimes() {
        return record.isOnlineDirectBuyHaveCoupon() ? 1 : 0;
    }

    /**
     * User buy online used coupon in 15 days from get the coupon
     *
     * @return buy online within 15 days times
     */
    public int getOnlineCouponBuyWithin15DaysTimes() {
        return record.isOnlineCouponBuyWithin15Days() ? 1 : 0;
    }

    /**
     * User buy online used coupon out of 15 days from get the coupon
     *
     * @return buy online out of 15 days times
     */
    public int getOnlineCouponBuyOutOf15DaysTimes() {
        return record.isOnlineCouponBuyOutOf15Days() ? 1 : 0;
    }

    @Override
    public String getKey() {
        return FeatureType.USER_BEHAVIOUR.toString();
    }

    @Override
    public String getValue() {
        StringBuilder builder = new StringBuilder(80);
        builder.append(record.getUserId()).append(CcfConstants.MAP_KEY_INNER_KEY_VALUE_SEPARATOR)
                .append(getOfflineDirectBuyWithoutCouponTimes()).append(CcfConstants.COLUMN_SEPARATOR)
                .append(getOfflineDirectBuyHaveCouponTimes()).append(CcfConstants.COLUMN_SEPARATOR)
                .append(getOfflineCouponBuyWithin15DaysTimes()).append(CcfConstants.COLUMN_SEPARATOR)
                .append(getOfflineCouponBuyOut15DaysTimes()).append(CcfConstants.COLUMN_SEPARATOR)
                .append(getOfflineCouponBuyAverageDistance()).append(CcfConstants.COLUMN_SEPARATOR)
                .append(getOfflineCouponBuyWithin15DaysAverageDiscount()).append(CcfConstants.COLUMN_SEPARATOR)
                .append(getOfflineCouponBuyWithin15DaysIsFixedDiscountTimes()).append(CcfConstants.COLUMN_SEPARATOR)
                .append(getOnlineClickCouponTimes()).append(CcfConstants.COLUMN_SEPARATOR)
                .append(getOnlineCollectCouponTimes()).append(CcfConstants.COLUMN_SEPARATOR)
                .append(getOnlineDirectBuyWithoutCouponTimes()).append(CcfConstants.COLUMN_SEPARATOR)
                .append(getOnlineDirectBuyHaveCouponTimes()).append(CcfConstants.COLUMN_SEPARATOR)
                .append(getOnlineCouponBuyWithin15DaysTimes()).append(CcfConstants.COLUMN_SEPARATOR)
                .append(getOnlineCouponBuyOutOf15DaysTimes());
        return builder.toString();
    }

    @Override
    public void reset() {
        this.validDistanceAccountNum = 0;
        this.validDiscountRateNum = 0;

        //Field
        this.offlineDirectBuyWithoutCouponTimes = 0;
        this.offlineDirectBuyHaveCouponTimes = 0;
        this.offlineCouponBuyWithin15DaysTimes = 0;
        this.offlineCouponBuyOut15DaysTimes = 0;
        this.offlineCouponBuyAverageDistance = 0;
        this.offlineCouponBuyWithin15DaysAverageDiscount = 0;
        this.offlineCouponBuyWithin15DaysIsFixedDiscountTimes = 0;
        this.onlineClickCouponTimes = 0;
        this.onlineCollectCouponTimes = 0;
        this.onlineDirectBuyWithoutCouponTimes = 0;
        this.onlineDirectBuyHaveCouponTimes = 0;
        this.onlineCouponBuyWithin15DaysTimes = 0;
        this.onlineCouponBuyOutOf15DaysTimes = 0;
    }

    @Override
    public void add(String line) {
        String[] info = CcfUtils.getRecordInfo(line, CcfConstants.COLUMN_SEPARATOR, 13);
        if (null == info) {
            LOG.error("Invalid input in UserBehaviourExtractable.add(" + line + ")");
            return;
        }

        //Field
        this.offlineDirectBuyWithoutCouponTimes += CcfUtils.getIntValue(info[0]);
        this.offlineDirectBuyHaveCouponTimes += CcfUtils.getIntValue(info[1]);
        this.offlineCouponBuyWithin15DaysTimes += CcfUtils.getIntValue(info[2]);
        this.offlineCouponBuyOut15DaysTimes += CcfUtils.getIntValue(info[3]);
        float distanceAverage = CcfUtils.getFloatValue(info[4]);
        if (distanceAverage > 0) {
            this.validDistanceAccountNum += 1;
            this.offlineCouponBuyAverageDistance = (this.offlineCouponBuyAverageDistance * (this
                    .validDistanceAccountNum - 1) + distanceAverage) / this.validDistanceAccountNum;
        }
        float discountAverage = CcfUtils.getFloatValue(info[5]);
        if (discountAverage > 0) {
            this.validDiscountRateNum += 1;
            this.offlineCouponBuyWithin15DaysAverageDiscount = (this.offlineCouponBuyWithin15DaysAverageDiscount * (this
                    .validDiscountRateNum - 1) + distanceAverage) / this.validDiscountRateNum;
        }
        this.offlineCouponBuyWithin15DaysIsFixedDiscountTimes += CcfUtils.getIntValue(info[6]);
        this.onlineClickCouponTimes += CcfUtils.getIntValue(info[7]);
        this.onlineCollectCouponTimes += CcfUtils.getIntValue(info[8]);
        this.onlineDirectBuyWithoutCouponTimes += CcfUtils.getIntValue(info[9]);
        this.onlineDirectBuyHaveCouponTimes += CcfUtils.getIntValue(info[10]);
        this.onlineCouponBuyWithin15DaysTimes += CcfUtils.getIntValue(info[11]);
        this.onlineCouponBuyOutOf15DaysTimes += CcfUtils.getIntValue(info[12]);
    }

    @Override
    public String getComputeResult() {
        StringBuilder builder = new StringBuilder(80);
        builder.append(this.offlineDirectBuyWithoutCouponTimes).append(CcfConstants.COLUMN_SEPARATOR)
                .append(this.offlineDirectBuyHaveCouponTimes).append(CcfConstants.COLUMN_SEPARATOR)
                .append(this.offlineCouponBuyWithin15DaysTimes).append(CcfConstants.COLUMN_SEPARATOR)
                .append(this.offlineCouponBuyOut15DaysTimes).append(CcfConstants.COLUMN_SEPARATOR)
                .append(this.offlineCouponBuyAverageDistance).append(CcfConstants.COLUMN_SEPARATOR)
                .append(this.offlineCouponBuyWithin15DaysAverageDiscount).append(CcfConstants.COLUMN_SEPARATOR)
                .append(this.offlineCouponBuyWithin15DaysIsFixedDiscountTimes).append(CcfConstants.COLUMN_SEPARATOR)
                .append(this.onlineClickCouponTimes).append(CcfConstants.COLUMN_SEPARATOR)
                .append(this.onlineCollectCouponTimes).append(CcfConstants.COLUMN_SEPARATOR)
                .append(this.onlineDirectBuyWithoutCouponTimes).append(CcfConstants.COLUMN_SEPARATOR)
                .append(this.onlineDirectBuyHaveCouponTimes).append(CcfConstants.COLUMN_SEPARATOR)
                .append(this.onlineCouponBuyWithin15DaysTimes).append(CcfConstants.COLUMN_SEPARATOR)
                .append(this.onlineCouponBuyOutOf15DaysTimes);
        return builder.toString();
    }
}
