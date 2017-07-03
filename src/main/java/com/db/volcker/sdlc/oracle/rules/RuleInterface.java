/**
 * QG rule interface.
 */
package com.db.volcker.sdlc.oracle.rules;

import java.util.List;
import java.util.Map;

import com.db.volcker.sdlc.oracle.domain.ErrMsgPO;
import com.db.volcker.sdlc.oracle.domain.PatternPO;

/**
 * @author garcluia
 *
 */
public interface RuleInterface {
    /** Excute the rule */
    public void executeRule();

    /**
     * Get error messages
     * 
     * @return
     */
    public List<ErrMsgPO> getErrorMessages();

    /**
     * Set include and exclude paths.
     * 
     * @param includePathsMap
     * @param excludePathsMap
     */
    public void addPaths(Map<Integer, PatternPO> includePathsMap, Map<Integer, PatternPO> excludePathsMap);

    /**
     * Check if rule is FATAL.
     * 
     * @return True or false
     */
    public boolean isFatal();
}
