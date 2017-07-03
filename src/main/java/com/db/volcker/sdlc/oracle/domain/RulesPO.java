/**
 * Rules PO.
 */
package com.db.volcker.sdlc.oracle.domain;

import java.util.ArrayList;
import java.util.List;

import com.db.volcker.sdlc.oracle.rules.RuleInterface;

/**
 * @author garcluia
 *
 */
public final class RulesPO {
    private List<RuleInterface> rules;

    /**
     * Default constructor.
     */
    public RulesPO() {
        /* EMPTY */
    }

    /**
     * @param rules
     */
    public RulesPO(List<RuleInterface> rules) {
        this.rules = rules;
    }

    /**
     * @return the rules
     */
    public List<RuleInterface> getRule() {
        return rules;
    }

    /**
     * @param rules
     *            the rules to set
     */
    public void setRule(List<RuleInterface> rules) {
        this.rules = rules;
    }

    /**
     * @param rules
     *            the rules to set
     */
    public void setRule(RuleInterface rule) {
        if (this.rules == null) {
            this.rules = new ArrayList<RuleInterface>();
        }

        this.rules.add(rule);
    }

    /**
     * @param rules
     *            the rules to set
     */
    public void addRule(RuleInterface rule) {
        if (this.rules == null) {
            this.rules = new ArrayList<RuleInterface>();
        }

        this.rules.add(rule);
    }

    /**
     * To string.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Defined rules:\n");
        for (RuleInterface ri : rules) {
            sb.append(ri.toString() + "\n");
        }
        return sb.toString();
    }
}
