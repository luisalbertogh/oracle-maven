/**
 * Configuration file PO.
 */
package com.db.volcker.sdlc.oracle.domain;

import java.util.HashMap;
import java.util.Map;

/**
 * @author garcluia
 *
 */
public final class ConfPO {
    private Map<Integer, PatternPO> includePathsMap = new HashMap<>();
    private Map<Integer, PatternPO> excludePathsMap = new HashMap<>();
    private IncludesPO includePaths;
    private ExcludesPO excludePatterns;
    private RulesPO rules;

    /**
     * @return the rules
     */
    public RulesPO getRules() {
        return rules;
    }

    /**
     * @param rules
     *            the rules to set
     */
    public void setRules(RulesPO rules) {
        this.rules = rules;
    }

    /**
     * @return the includePaths
     */
    public IncludesPO getIncludePaths() {
        return includePaths;
    }

    /**
     * @param includePaths
     *            the includePaths to set
     */
    public void setIncludePaths(IncludesPO includePaths) {
        this.includePaths = includePaths;
        for (PatternPO p : includePaths.getInPatternPO()) {
            this.includePathsMap.put(p.getId(), p);
        }
    }

    /**
     * @return the excludePatterns
     */
    public ExcludesPO getExcludePatterns() {
        return excludePatterns;
    }

    /**
     * @param excludePatterns
     *            the excludePatterns to set
     */
    public void setExcludePatterns(ExcludesPO excludePatterns) {
        this.excludePatterns = excludePatterns;
        for (PatternPO p : excludePatterns.getExPatternPO()) {
            this.excludePathsMap.put(p.getId(), p);
        }
    }

    /**
     * @return the includePathsMap
     */
    public Map<Integer, PatternPO> getIncludePathsMap() {
        return includePathsMap;
    }

    /**
     * @param includePathsMap
     *            the includePathsMap to set
     */
    public void setIncludePathsMap(Map<Integer, PatternPO> includePathsMap) {
        this.includePathsMap = includePathsMap;
    }

    /**
     * @return the excludePathsMap
     */
    public Map<Integer, PatternPO> getExcludePathsMap() {
        return excludePathsMap;
    }

    /**
     * @param excludePathsMap
     *            the excludePathsMap to set
     */
    public void setExcludePathsMap(Map<Integer, PatternPO> excludePathsMap) {
        this.excludePathsMap = excludePathsMap;
    }

    /**
     * To string.
     */
    @Override
    public String toString() {
        return rules.toString();
    }

}
