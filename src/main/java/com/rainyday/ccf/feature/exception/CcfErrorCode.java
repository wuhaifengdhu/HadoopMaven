package com.rainyday.ccf.feature.exception;

/**
 * Ccf error code.
 * @author haifwu
 */
public enum  CcfErrorCode {

    ERROR_NOT_BUILD(400, "Error happen extract features before build!"),

    ERROR_INPUT_FORMAT_ERROR(500, "Error input format");

    /**
     * code
     */
    private final int code;

    /**
     * description
     */
    private final String description;

    private CcfErrorCode(int code, String description){
        this.code = code;
        this.description = description;
    }

    /**
     * description getter
     *
     * @return
     */
    public String getDescription() {
        return description;
    }

    /**
     * code getter
     *
     * @return
     */
    public int getCode() {
        return code;
    }

    /**
     * user to string
     */
    @Override
    public String toString() {
        return code + ": " + description;
    }
}
