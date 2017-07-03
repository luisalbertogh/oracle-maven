/**
 * File and path matcher.
 */
package com.db.volcker.sdlc.oracle.utils;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;

/**
 * @author garcluia
 *
 */
public class FileMatcher {
    /** Path matchers */
    private PathMatcher sqlMatcher;
    private PathMatcher xmlMatcher;
    private PathMatcher deltaMatcher;
    private PathMatcher codeMatcher;
    private PathMatcher rollbackMatcher;

    /**
     * Default constructor.
     */
    public FileMatcher() {
        sqlMatcher = FileSystems.getDefault().getPathMatcher("glob:**.sql");
        xmlMatcher = FileSystems.getDefault().getPathMatcher("glob:**.xml");
        deltaMatcher = FileSystems.getDefault().getPathMatcher("glob:**/DELTA/**.sql");
        rollbackMatcher = FileSystems.getDefault().getPathMatcher("glob:**/DELTA/**/rollback/**.sql");
        codeMatcher = FileSystems.getDefault().getPathMatcher("glob:**/CODE/**.sql");
    }

    /**
     * Match DELTA rollback file path
     * 
     * @param file
     * @return
     */
    public boolean matchRollbackFile(Path file) {
        return rollbackMatcher.matches(file);
    }

    /**
     * Match SQL file path
     * 
     * @param file
     * @return
     */
    public boolean matchSQLFile(Path file) {
        return sqlMatcher.matches(file);
    }

    /**
     * Match XML file path
     * 
     * @param file
     * @return
     */
    public boolean matchXMLFile(Path file) {
        return xmlMatcher.matches(file);
    }

    /**
     * Match DELTA file path
     * 
     * @param file
     * @return
     */
    public boolean matchDELTAFile(Path file) {
        return deltaMatcher.matches(file);
    }

    /**
     * Match CODE file path
     * 
     * @param file
     * @return
     */
    public boolean matchCODEFile(Path file) {
        return codeMatcher.matches(file);
    }

}
