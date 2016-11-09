package com.rainyday.ccf.feature.container.data;

import com.rainyday.ccf.feature.util.CcfConstants;
import com.rainyday.ccf.feature.util.CcfUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author haifwu
 */
public class AbstractDataTest {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractDataTest.class);
    public static void main(String[] args) throws Exception {
//        System.out.println(getXTypeRate("0.3"));
//        System.out.println(getXYTypeRate("250:50"));
        System.out.println(getDiscountRate("250:50"));
        System.out.println(getDiscountRate("0.3"));
        System.out.println(getDiscountRate("fixed"));
        System.out.println(AbstractData.isFixedDiscountRateFromString("fixed"));
    }

    private static float getXYTypeRate(String rateString){
        float rate = 0;
        String[] xy = rateString.split(CcfConstants.RATE_SEPARATOR);
        if(xy.length != 2){
            // Input format error, return 0
            LOG.error("Input rate is x:y type, but not right. rateString = " + rateString);
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

    private static float getXTypeRate(String rateString){
        try{
            float x = Float.valueOf(rateString);
            return x;
        } catch (NumberFormatException ignore){
            // Invalid format input, return 0
            return 0;
        }
    }

    private static float getDiscountRate(String discountStr){
        return getDiscountRateFromString(discountStr);
    }

    public static float getDiscountRateFromString(String discountStr){
        if(CcfUtils.isNullValue(discountStr)){
            return 0;
        }
        if(AbstractData.isFixedDiscountRateFromString(discountStr)){
            return 0;
        }
        //TODO Maybe we can extract a feature for this type ratio, the same as fixed rate type
        if(discountStr.contains(CcfConstants.RATE_SEPARATOR)){
            return getXYTypeRate(discountStr);
        }
        return getXTypeRate(discountStr);
    }
}
