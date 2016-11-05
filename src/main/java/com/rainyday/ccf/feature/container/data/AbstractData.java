package com.rainyday.ccf.feature.container.data;

import com.rainyday.ccf.feature.exception.CcfErrorCode;
import com.rainyday.ccf.feature.exception.CcfException;
import com.rainyday.ccf.feature.util.CcfConstants;
import com.rainyday.ccf.feature.util.CcfUtils;

import java.util.Date;

/**
 * Abstract Data define a data type that provided method used for feature extraction.
 * To use this type data, should first call build method, which actually build an object.
 *
 * Field should be initialed in the construct method: record, separator
 * Field should be initialed in build method: userId, merchantId, discountStr,couponId, dateReceived, dateUsed
 *
 * @author haifwu
 */
public abstract class AbstractData implements Buildable{
    /**
     *  record contain this record's whole content
     */
    String record;
    /**
     * separator used to cut string record into fields
     */
    String separator;

    /**
     * user id, should not be null
     */
    String userId;
    /**
     *  merchant id, should not be null
     */
    String merchantId;
    /**
     * null if the user don't have a coupon
     */
    String couponId;
    /**
     * Four type discount:
     * (1) null means invalid value
     * (2) one single value means discount rate, 0.9 means 10% reduce
     * (3) [x:y] means y*1.0/x
     * (4) "fixed" means fixed type discount
     */
    String discountStr;
    /**
     *  null if the user don't have a coupon
     */
    Date dateReceived;
    /**
     *  null if this coupon not used, else the date use it
     */
    Date dateUsed;

    /**
     * Distance means how far this user willing to buy a thing around his home/company.
     * For offline data, it can return valid information.
     * For online training data, it return 0, means no value information.
     * @return distance value
     */
    public abstract int getDistance();

    /**
     * Get what type of this training data is.
     *
     * @return Online Data, or Offline Data
     */
    public abstract DataType getAbstractDataType();

    /**
     * Action type of online data, valid data [0, 1, 2], invalid format will set as -1
     *
     * @return action type, -1 if invalid
     */
    public abstract int getActionType();

    /**
     *  Method Area 1: For discount rate associate rate.
     */

    public float getDiscountRate(){
        if(CcfUtils.isNullValue(discountStr)){
            return 0;
        }
        if(isFixedDiscountRate()){
            return 0;
        }
        //TODO Maybe we can extract a feature for this type ratio, the same as fixed rate type
        if(discountStr.contains(CcfConstants.RATE_SEPARATOR)){
            return getXYTypeRate(discountStr);
        }
        return getXTypeRate(discountStr);
    }

    private float getXYTypeRate(String rateString){
        float rate = 0;
        String[] xy = rateString.split(CcfConstants.RATE_SEPARATOR);
        if(xy.length != 2){
            // Input format error, return 0
            return 0;
        }
        try{
            int x = Integer.valueOf(xy[0]);
            int y = Integer.valueOf(xy[1]);
            rate = (float) (y * 1.0 / x);
        } catch (NumberFormatException ignore){
            return 0;
        }
        return rate;
    }

    private float getXTypeRate(String rateString){
        try{
            float x = Float.valueOf(rateString);
            return x;
        } catch (NumberFormatException ignore){
            // Invalid format input, return 0
            return 0;
        }
    }

    public boolean isFixedDiscountRate(){
        if(not(CcfUtils.isNullValue(discountStr)) && CcfConstants.FIXED_DISCOUNT.equalsIgnoreCase(discountStr)){
            return true;
        }
        return false;
    }


    /**
     *  Method Area 2: Variables for Coupon Tendency and merchant behaviour features
     */
    public boolean isCouponBeingUsedOfflineWithin15Days() {
        return isOfflineCouponBuy() && within15Days();
    }

    public boolean isCouponBeingUsedOfflineOutOf15Days() {
        return isOfflineCouponBuy() && not(within15Days());
    }

    /**
     *  Method Area 3: Variables for User Behaviour and merchant behaviour features
     */
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
     * Method Area 3:  Common function attached to the logical of CCF project
     *
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
        return not(CcfUtils.isNullValue(this.couponId)) && not(CcfUtils.isNullValue(this.dateReceived));
    }

    private boolean useCoupon() {
        return not(CcfUtils.isNullValue(this.dateUsed));
    }

    private boolean isOnlineType() {
        return DataType.ONLINE.equals(getAbstractDataType());
    }

    private boolean isOfflineType() {
        return DataType.OFFLINE.equals(getAbstractDataType());
    }

    private boolean within15Days(){
        return CcfUtils.dateDiffWithin15Days(dateReceived, dateUsed);
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

    protected boolean not(boolean value){
        return ! value;
    }

    /**
     * Method Area 4:  Common Getter, Setter
     */

    public String getUserId() {
        return userId;
    }

    public String getMerchantId() {
        return merchantId;
    }

    public String getCouponId() {
        return couponId;
    }

    /**
     * Method Area 5: Data valid check
     */
    public boolean inputValidCheck(){
         if( CcfUtils.isNullValue(this.record) || CcfUtils.isNullValue(this.userId) || CcfUtils.isNullValue(this
                 .merchantId) ){
             return false;
         }
         return true;
    }
}
