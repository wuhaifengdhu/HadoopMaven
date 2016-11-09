package com.rainyday.ccf.feature.util;

/**
 * Constant values for CCF project.
 *
 * @author haifwu
 */
public interface CcfConstants {
    public static final String EMPTY_STRING = "";
    public static final String NULL_VALUE="null";
    public static final String LINE_SEPARATOR_KEY = "line.separator";
    public static final String DEFAULT_LINE_SEPARATOR = ",";
    public static final String INPUT_TRAINING_DATA_TYPE = "input.training.data.type";
    public static final String INPUT_PATH = "input.path";
    public static final String OUTPUT_PATH = "output.path";
    public static final String LOG_PREFIX = "CCF-LOG-PREFIX:";
    public static final String COLUMN_SEPARATOR = "|";
    public static final String KEY_VALUE_SEPARATOR = "|";
    public static final String MAP_KEY_INNER_KEY_VALUE_SEPARATOR = "#";

    public static final int ACTION_CLICK = 0;
    public static final int ACTION_BUY = 1;
    public static final int ACTION_COLLECT = 2;
    public static final int ACTION_NULL = -1;

    public static final String FIXED_DISCOUNT = "fixed";
    public static final String RATE_SEPARATOR = ":";

    public static final String DEFAULT_DATE_FORMAT = "yyyyMMdd";

    public static final String DEFAULT_ONLINE_FOLDER_NAME = "online";
    public static final String DEFAULT_OFFLINE_FOLDER_NAME = "offline";
    public static final String ONLINE_FOLDER_NAME_PARA = "online.folder.name";
    public static final String OFFLINE_FOLDER_NAME_PARA = "offline.folder.name";

    public static final String EXTRACTION_FEATURE_TYPE_PARA = "extraction.feature.type";
    public static final int DAYS_LIMIT = 15;
}
