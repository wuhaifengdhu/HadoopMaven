package com.rainyday.ccf.feature.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Class with static method may be used in project.
 *
 * @author haifwu
 */
public final class CcfUtils {
    private static final Logger LOG = LoggerFactory.getLogger(CcfUtils.class);

    public static long dateDiff(String receiveDate, String useDate) throws ParseException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        Date received = dateFormat.parse(receiveDate);
        Date used = dateFormat.parse(useDate);
        return dateDiff(received, used);
    }

    public static boolean useCouponWithinDays(String receiveDate, String useDate, int maxDiffDays) {
        // situation 1, user use
        if (CcfConstants.NULL_VALUE.equalsIgnoreCase(receiveDate) && null != useDate) {
            return true;
        } else {
            try {
                long diffDays = dateDiff(receiveDate, useDate);
                if (diffDays > 0 && diffDays <= maxDiffDays) {
                    return true;
                }
            } catch (ParseException e) {
                LOG.error(CcfConstants.LOG_PREFIX + "Error parse date in CcfUtils.useCouponWithinDays", e);
            }
        }
        return false;
    }

    public static long dateDiff(Date received, Date used){
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(received);
        long receiveTime = calendar.getTimeInMillis();
        calendar.setTime(used);
        long usedTime = calendar.getTimeInMillis();
        return (usedTime - receiveTime) / (24 * 3600 * 1000);
    }

    public static boolean dateDiffWithin15Days(Date received, Date used){
        return dateDiff(received, used) <= 15;
    }
}
