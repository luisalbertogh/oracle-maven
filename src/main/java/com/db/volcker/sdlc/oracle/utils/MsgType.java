/**
 * Message type.
 */
package com.db.volcker.sdlc.oracle.utils;

/**
 * @author garcluia
 *
 */
public enum MsgType {
    WARNING("warn", "Do not set as error"), ERROR("error", "Set as error but do not stop"), FATAL("fatal",
            "Set as error and stop");

    private String id;
    private String descr;

    MsgType(String id, String descr) {
        this.id = id;
    }

    public String getId() {
        return this.id;
    }

    public String getDescr() {
        return this.descr;
    }
}
