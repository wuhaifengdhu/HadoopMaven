package com.rainyday.ccf;

import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.logging.Logger;

/**
 *  Class with static method may be used in project.
 *
 * @author haifwu
 */
public class CcfUtils {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(CcfUtils.class);

    public static int diffDays(String receiveDate, String useDate) throws ParseException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        Calendar cal = Calendar.getInstance();
        cal.setTime(dateFormat.parse(receiveDate));
        long receiveTime = cal.getTimeInMillis();
        cal.setTime(dateFormat.parse(useDate));
        long useTime = cal.getTimeInMillis();
        long diffDays = (useTime - receiveTime)/(24 * 3600 * 1000);
        return Integer.parseInt(String.valueOf(diffDays));
    }

    public static boolean useCouponWithinDays(String receiveDate, String useDate, int maxDiffDays){
        // situation 1, user use
        if("null".equalsIgnoreCase(receiveDate) && null != useDate){
            return true;
        } else {
            try {
                int diffDays = diffDays(receiveDate, useDate);
                if(diffDays > 0 && diffDays <= maxDiffDays){
                    return true;
                }
            } catch (ParseException e) {
                LOG.error("Error parse date in CcfUtils.useCouponWithinDays", e);
            }
        }
        return false;
    }

}
