package com.rainyday.ccf.feature.util;

import com.rainyday.ccf.feature.container.extractable.FeatureType;
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
    private static SimpleDateFormat dateFormat = new SimpleDateFormat(CcfConstants.DEFAULT_DATE_FORMAT);

    public static long dateDiff(String receiveDate, String useDate) throws ParseException {
        Date received = dateFormat.parse(receiveDate);
        Date used = dateFormat.parse(useDate);
        return dateDiff(received, used);
    }

    public static boolean dateDiffWithinDays(String receiveDate, String useDate, int maxDiffDays) {
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

    public static long dateDiff(Date received, Date used) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(received);
        long receiveTime = calendar.getTimeInMillis();
        calendar.setTime(used);
        long usedTime = calendar.getTimeInMillis();
        return (usedTime - receiveTime) / (24 * 3600 * 1000);
    }

    public static boolean dateDiffWithin15Days(Date received, Date used) {
        return dateDiff(received, used) <= 15;
    }

    public static boolean couponUsedWithinDays(String receivedDate, String usedDate, int days){
        if(CcfUtils.isNullValue(receivedDate) || CcfUtils.isNullValue(usedDate)){
            return false;
        }
        try {
            Date received = dateFormat.parse(receivedDate);
            Date used = dateFormat.parse(usedDate);
            return dateDiff(received, used) <= days;
        } catch (ParseException ignore) {
        }
        return false;
    }

    public static boolean isNullValue(Object value) {
        return null == value || CcfConstants.NULL_VALUE.equals(value.toString());
    }

    public static int getIntValue(String value) {
        if (isNullValue(value)) {
            return 0;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException ignore) {
            LOG.error("Invalid value convert to int: " + value);
            return 0;
        }
    }

    public static Date getDateValue(String value) {
        if(isNullValue(value)){
            return null;
        }
        try {
            return dateFormat.parse(value);
        } catch (ParseException ignore) {
            LOG.error("Invalid value convert to Date: " + value);
            return null;
        }
    }

    public static float getFloatValue(String value){
        if (isNullValue(value)) {
            return -1;
        }
        try {
            return Float.parseFloat(value);
        } catch (NumberFormatException ignore) {
            LOG.error("Invalid value convert to float: " + value);
            return -1;
        }
    }

    public static String[] getRecordInfo(String line, String separator, int validFieldNum) {
        if (null == line || null == separator) {
            LOG.error("Call getRecordInfo method with invalid input parameters<" + line + ", " + separator + ", " + validFieldNum);
            return null;
        }
        //TODO we need fixed this, not all separator need \\, we use | that need it
        String[] info = line.split("\\" + separator);
        if (info.length != validFieldNum) {
            LOG.error("info.length = " + info.length + " required:" + validFieldNum + " for line:" + line);
            return null;
        }
        return info;
    }

    public static String getNoNullString(Object object){
        return null == object ? CcfConstants.EMPTY_STRING : object.toString();
    }

    public static FeatureType getFeatureTypeFromString(String type){
        if(null == type){
            return null;
        }
        try {
            FeatureType featureType = FeatureType.valueOf(type);
            return featureType;
        } catch (IllegalArgumentException ignore){
        }
        return null;
    }
}
