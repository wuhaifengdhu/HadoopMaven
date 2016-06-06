package com.haifwu.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public final class Common {
    public static long convertDateToPIT(String dateStr, String format) throws ParseException {
        DateFormat dataFormat = new SimpleDateFormat(format);
        return dataFormat.parse(dateStr).getTime();
    }
    
    public static String getLastStringBetween(String line, String theStringBefore, String theStringAfter){
        int start = line.lastIndexOf(theStringBefore);
        int end = line.lastIndexOf(theStringAfter);
        if(start != -1 && end != -1 && start < end){
            return line.substring(start + theStringBefore.length(), end).trim();
        }
        return null;
    }
}
