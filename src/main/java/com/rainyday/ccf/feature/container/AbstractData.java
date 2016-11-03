package com.rainyday.ccf.feature.container;

import com.rainyday.ccf.feature.util.CcfConstants;
import com.rainyday.ccf.feature.util.CcfUtils;

import java.util.Date;

/**
 * @author haifwu
 */
public abstract class AbstractData {

    public abstract String getUserId();

    public abstract String getMerchantId();

    public abstract String getCouponId();

    public abstract float getDiscountRate();

    public abstract boolean isFixedDiscountRate();

    public abstract int getDistance();

    public abstract Date getDateReceived();

    /**
     * Get the date this coupon being used
     *
     * @return null if this coupon not used, else the date use it
     */
    public abstract Date getDateUsed();

    public abstract DataType getAbstractDataType();

    /**
     * Action type of online data, valid data [0, 1, 2], invalid format will set as -1
     *
     * @return action type, -1 if invalid
     */
    public abstract int getActionType();

    // Variables for Coupon Tendency features
    public boolean isCouponBeingUsedOfflineWithin15Days() {
        return isOfflineCouponBuy() && within15Days();
    }

    public boolean isCouponBeingUsedOfflineOutOf15Days() {
        return isOfflineCouponBuy() && not(within15Days());
    }

    // Variables for User Behaviour features
    public boolean isOfflineDirectBuyWithoutCoupon() {
        return isOfflineBuy() && not(useCoupon()) && not(hasCoupon());
    }

    public boolean isOfflineDirectBuyHaveCoupon() {
        return isOfflineBuy() && not(useCoupon()) && hasCoupon();
    }

    public boolean isOfflineCouponBuyWithin15Days() {
        return isOfflineCouponBuy() && within15Days();
    }

    public boolean isOfflineCouponBuyOutOf15Days() {
        return isOfflineCouponBuy() && not(within15Days());
    }

    public boolean isOfflineCouponBuy() {
        return isOfflineBuy() && hasCoupon() && useCoupon();
    }

    public boolean isOnlineClickCouponType() {
        return isOnlineType() && isActionClick();
    }

    public boolean isOnlineCollectCouponType() {
        return isOnlineType() && isActionCollect();
    }

    public boolean isOnlineDirectBuyWithoutCoupon() {
        return isOnlineBuy() && not(useCoupon()) && not(hasCoupon());
    }

    public boolean isOnlineDirectBuyHaveCoupon(){
        return isOnlineBuy() && not(useCoupon()) && hasCoupon();
    }

    public boolean isOnlineCouponBuyWithin15Days(){
        return isOnlineBuy() && useCoupon() && hasCoupon() && within15Days();
    }

    public boolean isOnlineCouponBuyOutOf15Days(){
        return isOnlineBuy() && useCoupon() && hasCoupon() && within15Days();
    }

    /**
     * Comments for the following function!
     * <p>
     * For online data:
     * action type decide the event is click, buy, or collect.
     * coupon_id is null or not decided the user has coupon or not
     * DateUsed is null or not decide the user use coupon or not
     * <p>
     * <p>
     * For offline training data, each records is a buy action records. Offline direct buy without coupon, we should
     * check if he doesn't have coupon and doesn't use it.
     * coupon_id is null or not decide whether he has coupon or not.
     * DateUsed is null or not decide whether he use coupon or not.
     */
    private boolean hasCoupon() {
        return getCouponId() != null && getDateReceived() != null;
    }

    private boolean useCoupon() {
        return getDateUsed() != null;
    }

    private boolean isOnlineType() {
        return DataType.ONLINE.equals(getAbstractDataType());
    }

    private boolean isOfflineType() {
        return DataType.OFFLINE.equals(getAbstractDataType());
    }

    private boolean within15Days(){
        return CcfUtils.dateDiffWithin15Days(getDateReceived(), getDateUsed());
    }

    public boolean isActionCollect() {
        return getActionType() == CcfConstants.ACTION_COLLECT;
    }

    public boolean isActionClick() {
        return getActionType() == CcfConstants.ACTION_CLICK;
    }

    public boolean isActionBuy() {
        return getActionType() == CcfConstants.ACTION_BUY;
    }

    public boolean isOnlineBuy() {
        return isOnlineType() && isActionBuy();
    }

    public boolean isOfflineBuy() {
        return isOfflineType();
    }

    private boolean not(boolean value){
        return ! value;
    }
}
