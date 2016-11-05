package com.rainyday.ccf.feature.exception;

/**
 * CcfException, contain error code.
 *
 * @author haifwu
 */
public class CcfException extends RuntimeException{
    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = 320754504510306120L;

    /**
     * error code
     */
    private CcfErrorCode error = null;

    /**
     * Constructor
     *
     * @param code
     */
    public CcfException(CcfErrorCode code) {
        super();
        setError(code);
    }

    public CcfException(CcfErrorCode code, Exception e) {
        super(e);
        this.setError(code);
    }

    /**
     * Constructor
     *
     * @param code
     * @param msg
     */
    public CcfException(CcfErrorCode code, String msg) {
        super(msg);
        this.setError(code);
    }

    /**
     * Constructor
     *
     * @param code
     * @param e
     * @param msg
     */
    public CcfException(CcfErrorCode code, Exception e, String msg) {
        super(msg, e);
        this.setError(code);
    }

    public CcfErrorCode getError() {
        return error;
    }

    public void setError(CcfErrorCode error) {
        this.error = error;
    }

    @Override
    public String toString() {
        return "CcfException [error=" + error + "]";
    }

}
