package com.rainyday.ccf.feature.container.extractable;

import com.rainyday.ccf.feature.container.data.AbstractData;
import com.rainyday.ccf.feature.util.CcfConstants;

/**
 * Created by haifwu on 2016/11/1.
 */
public class UserBehaviourExtractable implements Extractable {
    private AbstractData record;

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
}
