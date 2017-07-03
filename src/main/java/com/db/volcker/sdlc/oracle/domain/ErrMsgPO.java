/**
 * Error message PO.
 */
package com.db.volcker.sdlc.oracle.domain;

import java.nio.file.Path;

/**
 * @author garcluia
 *
 */
public final class ErrMsgPO {
    private String ruleName;
    private Path file;
    private int nline;
    private String currLine;
    private String msg;
    private String type;

    /**
     * Default constructor.
     */
    public ErrMsgPO() {
        /* EMPTY */
    }

    /**
     * @return the type
     */
    public String getType() {
        return type;
    }

    /**
     * @param type
     *            the type to set
     */
    public void setType(String type) {
        this.type = type;
    }

    /**
     * @param file
     * @param nline
     * @param currLine
     * @param msg
     */
    public ErrMsgPO(String ruleName, Path file, int nline, String currLine, String msg, String type) {
        this.ruleName = ruleName;
        this.file = file;
        this.nline = nline;
        this.currLine = currLine;
        this.msg = msg;
        this.type = type;
    }

    /**
     * @return the file
     */
    public Path getFile() {
        return file;
    }

    /**
     * @param file
     *            the file to set
     */
    public void setFile(Path file) {
        this.file = file;
    }

    /**
     * @return the nline
     */
    public int getNline() {
        return nline;
    }

    /**
     * @param nline
     *            the nline to set
     */
    public void setNline(int nline) {
        this.nline = nline;
    }

    /**
     * @return the currLine
     */
    public String getCurrLine() {
        return currLine;
    }

    /**
     * @param currLine
     *            the currLine to set
     */
    public void setCurrLine(String currLine) {
        this.currLine = currLine;
    }

    /**
     * @return the msg
     */
    public String getMsg() {
        return msg;
    }

    /**
     * @param msg
     *            the msg to set
     */
    public void setMsg(String msg) {
        this.msg = msg;
    }

    /**
     * To string.
     */
    @Override
    public String toString() {
        return this.ruleName + ": Error in " + this.file + " - Line: " + this.nline + " - Wrong content: '"
                + this.currLine + "' - Error message: " + this.msg;
    }

}
