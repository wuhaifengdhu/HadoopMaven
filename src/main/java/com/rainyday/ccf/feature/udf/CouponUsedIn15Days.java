package com.rainyday.ccf.feature.udf;
import com.rainyday.ccf.feature.util.CcfConstants;
import com.rainyday.ccf.feature.util.CcfUtils;
import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;

/**
 * @author haifwu
 */
public class CouponUsedIn15Days extends EvalFunc<String>{
    private static final Logger LOG = Logger.getLogger(CouponUsedIn15Days.class);

    @Override
    public String exec(Tuple input) throws IOException {
        String receivedDate = (String) input.get(0);
        String usedDate = (String) input.get(1);
        if(CcfUtils.isNullValue(receivedDate) || CcfUtils.isNullValue(usedDate)){
            return "0";
        }
        return CcfUtils.couponUsedWithinDays(receivedDate, usedDate, CcfConstants.DAYS_LIMIT) ? "1" : "0";
    }
}
