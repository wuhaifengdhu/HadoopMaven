package com.rainyday.ccf.feature.container;

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
     * @return clicked times
     */
    int getClickedTimes(){
        return record.isClick() ? 1 : 0;
    }

    /**
     *  How many time this merchant being collected by users
     * @return collected times
     */
    int getCollectTimes(){
        return record.isCollect() ? 1 : 0;
    }

    /**
     *  How many times this merchant being used within 15 days
     * @return used within 15 days times
     */
    int getUsedWithin15DaysTimes(){
        return record.isCouponBeingUsedWithin15Days() ? 1 : 0;
    }

    /**
     *  How many times this merchant being used out of 15 days
     * @return
     */
    int getUsedOutOf15DaysTimes(){
        return record.isCouponBeingUsedOutOf15Days() ? 1 : 0;
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
        return record.getMerchantId();
    }

    @Override
    public String getValue() {
        StringBuilder builder = new StringBuilder(50);
        builder.append(getRecordTimes()).append(CcfConstants.COLUMN_SEPARATOR)
                .append(getClickedTimes()).append(CcfConstants.COLUMN_SEPARATOR)
                .append(getCollectTimes()).append(CcfConstants.COLUMN_SEPARATOR)
                .append(getUsedWithin15DaysTimes()).append(CcfConstants.COLUMN_SEPARATOR)
                .append(getUsedOutOf15DaysTimes()).append(CcfConstants.COLUMN_SEPARATOR)
                .append(getAverageDiscountRate());
        return builder.toString();
    }
}
