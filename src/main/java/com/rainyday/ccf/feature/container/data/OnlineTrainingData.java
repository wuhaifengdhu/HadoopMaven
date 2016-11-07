package com.rainyday.ccf.feature.container.data;

import com.rainyday.ccf.feature.util.CcfConstants;
import com.rainyday.ccf.feature.util.CcfUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author haifwu
 */
public class OnlineTrainingData extends AbstractData {
    private static final Logger LOG = LoggerFactory.getLogger(OnlineTrainingData.class);
    /**
     * Online data special field
     */
    private int actionType;

    public OnlineTrainingData(String line, String separator) {
        this.record = line;
        this.separator = separator;
    }

    /**
     * For online date, his offline buy distance is always 0
     *
     * @return 0
     */
    @Override
    public int getDistance() {
        return 0;
    }

    @Override
    public DataType getAbstractDataType() {
        return DataType.ONLINE;
    }

    @Override
    public int getActionType() {
        return this.actionType;
    }

    /**
     * Online Data record: [user_id, merchant_id, action, coupon_id, discount_rate, date_received, date]
     *
     * @return AbstractData being build, null if build failure
     */
    @Override
    public AbstractData build() {
        String[] info = CcfUtils.getRecordInfo(this.record, this.separator, 7);
        if (null == info) {
            return null;
        }
        // set value for each column
        this.userId = info[0];
        this.merchantId = info[1];
        this.actionType = convertToActionType(info[2]);
        this.couponId = info[3];
        this.discountStr = info[4];
        this.dateReceived = CcfUtils.getDateValue(info[5]);
        this.dateUsed = CcfUtils.getDateValue(info[6]);

        // data valid check
        if(not(inputValidCheck())){
            return null;
        }
        return this;
    }

    private int convertToActionType(String value) {
        if(CcfUtils.isNullValue(value)) return CcfConstants.ACTION_NULL;
        int actionValue;
        try {
            actionValue = Integer.parseInt(value);
        } catch (NumberFormatException ignore) {
            LOG.error("convert string to action type failed, value: " + value);
            return CcfConstants.ACTION_NULL;
        }
        if(actionValue > 2 || actionValue < 0){
            return CcfConstants.ACTION_NULL;
        }
        return actionValue;
    }
}
