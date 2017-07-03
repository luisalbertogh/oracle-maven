/**
 * Include pattern PO.
 */
package com.db.volcker.sdlc.oracle.domain;

/**
 * @author garcluia
 *
 */
public final class PatternPO {
    private int id;
    private String pathPattern;

    /**
     * @return the pathPattern
     */
    public String getPathPattern() {
        return pathPattern;
    }

    /**
     * @param pathPattern
     *            the pathPattern to set
     */
    public void setPathPattern(String pathPattern) {
        this.pathPattern = pathPattern;
    }

    /**
     * @return the id
     */
    public int getId() {
        return id;
    }

    /**
     * @param id
     *            the id to set
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * To string.
     */
    @Override
    public String toString() {
        return "Path pattern: " + this.pathPattern;
    }
}
