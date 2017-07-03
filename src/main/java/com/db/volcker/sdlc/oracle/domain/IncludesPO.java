/**
 * Include path patterns PO.
 */
package com.db.volcker.sdlc.oracle.domain;

import java.util.ArrayList;
import java.util.List;

/**
 * @author garcluia
 *
 */
public final class IncludesPO {
    private PatternPO singlePathPO;
    private List<PatternPO> inPatternPO;

    /**
     * Check if list is empty.
     * 
     * @return
     */
    public boolean isEmpty() {
        if (inPatternPO == null || (inPatternPO != null && inPatternPO.size() == 0)) {
            return true;
        }

        return false;
    }

    /**
     * @return the singlePathPO
     */
    public PatternPO getSinglePathPO() {
        return singlePathPO;
    }

    /**
     * @param singlePathPO
     *            the singlePathPO to set
     */
    public void setSinglePathPO(PatternPO singlePathPO) {
        this.singlePathPO = singlePathPO;
        if (inPatternPO == null) {
            this.inPatternPO = new ArrayList<PatternPO>();
        }
        this.inPatternPO.add(singlePathPO);
    }

    /**
     * @return the inPatternPO
     */
    public List<PatternPO> getInPatternPO() {
        return inPatternPO;
    }

    /**
     * @param inPatternPO
     *            the inPatternPO to set
     */
    public void setInPatternPO(PatternPO inPatternPO) {
        if (this.inPatternPO == null) {
            this.inPatternPO = new ArrayList<PatternPO>();
        }
        this.inPatternPO.add(inPatternPO);
    }

    /**
     * @param inPatternPO
     *            the inPatternPO to set
     */
    public void setInPatternPO(List<PatternPO> inPatternPO) {
        this.inPatternPO = inPatternPO;
    }

    /**
     * To string.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (this.inPatternPO != null) {
            for (PatternPO p : inPatternPO) {
                sb.append(p + "\n");
            }
        } else {
            sb.append(this.singlePathPO.toString());
        }
        return sb.toString();
    }
}
