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
     *  Get the date this coupon being used
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

    public boolean isClick() {
        if (getActionType() == CcfConstants.ACTION_CLICK) {
            return true;
        }
        return false;
    }

    public boolean isCollect() {
        if (getActionType() == CcfConstants.ACTION_COLLECT) {
            return true;
        }
        return false;
    }

    public boolean useCouponBuyWith15Days() {
        return getActionType() == CcfConstants.ACTION_BUY && CcfUtils.dateDiffWithin15Days(getDateReceived(),
                getDateUsed());
    }

    public boolean useCouponOutOf15Days() {
        return getActionType() == CcfConstants.ACTION_BUY && !CcfUtils.dateDiffWithin15Days(getDateReceived(),
                getDateUsed());
    }

    public boolean isOfflineDirectBuyWithoutCoupon() {
        return DataType.OFFLINE.equals(getAbstractDataType()) && getDateUsed() != null && getCouponId() == null
                && getDateReceived() == null;
    }

    public boolean isOfflineDirectBuyHaveCoupon() {
        return DataType.OFFLINE.equals(getAbstractDataType()) && getDateUsed() == null && getCouponId() != null
                && getDateReceived() != null;
    }

    public boolean isOfflineCouponBuyWithin15Days(){
        return DataType.OFFLINE.equals(getAbstractDataType()) && getDateUsed() != null && getCouponId() != null
                && getDateReceived() != null;
    }

}
