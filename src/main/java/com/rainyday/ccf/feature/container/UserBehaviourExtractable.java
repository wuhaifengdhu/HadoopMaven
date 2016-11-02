package com.rainyday.ccf.feature.container;

/**
 * Created by haifwu on 2016/11/1.
 */
public class UserBehaviourExtractable implements Extractable{
    private AbstractData record;

    public UserBehaviourExtractable(AbstractData data){
        this.record = data;
    }

    /**
     *  User direct buy things offline, and he/she doesn't have coupon
     * @return directly buy without coupon in hand times
     */
    public int getOfflineDirectBuyWithoutCouponTimes(){
        return record.isOfflineDirectBuyWithoutCoupon() ? 1 : 0;
    }

    /**
     *  User direct buy things offline, through he/she has coupon ticket
     * @return directly buy with coupon in hand times
     */
    public int getOfflineDirectBuyHaveCouponTimes(){
        return record.isOfflineDirectBuyHaveCoupon() ? 1 : 0;
    }

    /**
     *  User buy offline used coupon in 15 days from get the coupon
     * @return buy offline within 15 days times
     */
    public int getOfflineCouponBuyWithin15DaysTimes();

    /**
     *  User buy offline used coupon out of 15 days from get the coupon
     * @return buy offline out of 15 days times
     */
    public int getOfflineCouponBuyOut15DaysTimes();

    /**
     *  Calculate the average distance the user willing to buy offline use coupon
     * @return average distance the user willing to buy
     */
    public int getOfflineCouponBuyAverageDistance();

    /**
     *  User offline buy use coupon with 15 days' average coupon
     * @return offline user buy with coupon average discount
     */
    float getOfflineCouponBuyWithin15DaysAverageDiscount();

    /**
     *  User offline buy use fixed type coupon times
     * @return offline coupon buy is fixed discount times
     */
    public int getOfflineCouponBuyWithin15DaysIsFixedDiscountTimes();

    /**
     *  User click the coupon but not collect or use it.
     * @return online click coupon times
     */
    public int getOnlineClickCouponTimes();

    /**
     *  User collect coupon but not use it.
     *  @return online collect coupon times
     */
    public int getOnlineCollectCouponTimes();

    /**
     *  User direct buy things online, and he/she doesn't have coupon
     * @return directly buy without coupon in hand times
     */
    public int getOnlineDirectBuyWithoutCouponTimes();

    /**
     *  User direct buy things online, through he/she has coupon ticket
     * @return directly buy with coupon in hand times
     */
    public int getOnlineDirectBuyHaveCouponTimes();

    /**
     *  User buy online used coupon in 15 days from get the coupon
     * @return buy online within 15 days times
     */
    public int getOnlineCouponBuyWithin15DaysTimes();

    /**
     *  User buy online used coupon out of 15 days from get the coupon
     * @return buy online out of 15 days times
     */
    public int getOnlineCouponBuyOut15DaysTimes();

    @Override
    public String getKey() {
        return re;
    }

    @Override
    public String getValue() {
        return null;
    }
}
