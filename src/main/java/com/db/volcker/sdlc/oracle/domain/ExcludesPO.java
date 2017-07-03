/**
 * 
 */
package com.db.volcker.sdlc.oracle.domain;

import java.util.List;

/**
 * @author garcluia
 *
 */
public final class ExcludesPO {
    private List<PatternPO> exPatternPO;

    /**
     * Check if list is empty.
     * 
     * @return
     */
    public boolean isEmpty() {
        if (exPatternPO == null || (exPatternPO != null && exPatternPO.size() == 0)) {
            return true;
        }

        return false;
    }

    /**
     * @return the exPatternPO
     */
    public List<PatternPO> getExPatternPO() {
        return exPatternPO;
    }

    /**
     * @param exPatternPO
     *            the exPatternPO to set
     */
    public void setExPatternPO(List<PatternPO> exPatternPO) {
        this.exPatternPO = exPatternPO;
    }

    /**
     * To string.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Exclude patterns:\n");
        for (PatternPO p : exPatternPO) {
            sb.append(p + "\n");
        }
        return sb.toString();
    }
}
