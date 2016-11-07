package com.rainyday.ccf.feature.container.data;

import com.rainyday.ccf.feature.exception.CcfErrorCode;
import com.rainyday.ccf.feature.exception.CcfException;
import com.rainyday.ccf.feature.util.CcfConstants;
import com.rainyday.ccf.feature.util.CcfUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author haifwu
 */
public class OfflineTrainingData extends AbstractData{
    private static final Logger LOG = LoggerFactory.getLogger(OfflineTrainingData.class);
    /**
     *  Offline data special field
     */
    private int distance;

    public OfflineTrainingData(String line, String separator){
        this.record = line;
        this.separator = separator;
    }

    @Override
    public int getDistance() {
        return this.distance;
    }

    @Override
    public DataType getAbstractDataType() {
        return DataType.OFFLINE;
    }

    /**
     * For offline data, there is no action type
     * @return ACTION_NULL
     */
    @Override
    public int getActionType() {
        return CcfConstants.ACTION_NULL;
    }

    /**
     * Offline Data record: [user_id, merchant_id, coupon_id, discount_rate, distance, date_received, date]
     * @return
     */
    @Override
    public AbstractData build() {
        String[] info = CcfUtils.getRecordInfo(this.record, this.separator, 7);
        if(null == info){
            return null;
        }
        // set value for each column
        this.userId = info[0];
        this.merchantId = info[1];
        this.couponId = info[2];
        this.discountStr = info[3];
        this.distance = CcfUtils.getIntValue(info[4]);
        this.dateReceived = CcfUtils.getDateValue(info[5]);
        this.dateUsed = CcfUtils.getDateValue(info[6]);

        // data valid check
        if(not(inputValidCheck())){
            return null;
        }
        return this;
    }
}
