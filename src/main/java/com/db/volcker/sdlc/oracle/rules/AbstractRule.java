/**
 * Abstract rule class.
 */
package com.db.volcker.sdlc.oracle.rules;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.dom4j.Element;
import org.w3c.dom.Comment;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.db.volcker.sdlc.oracle.domain.ErrMsgPO;
import com.db.volcker.sdlc.oracle.domain.ExcludesPO;
import com.db.volcker.sdlc.oracle.domain.IncludesPO;
import com.db.volcker.sdlc.oracle.domain.PatternPO;
import com.db.volcker.sdlc.oracle.utils.FileMatcher;
import com.db.volcker.sdlc.oracle.utils.MavenLoggerBridge;
import com.db.volcker.sdlc.oracle.utils.MsgType;

import jregex.Matcher;
import jregex.Pattern;
import jregex.REFlags;

/**
 * @author garcluia
 *
 */
public abstract class AbstractRule extends SimpleFileVisitor<Path> {
    /* Rules basic attributes */
    protected int line;
    protected String attrPattern;
    protected String pathPattern;
    protected String id;
    protected String fileExt;
    protected String contentPattern;
    protected String errMsg;
    protected IncludesPO includePaths;
    protected ExcludesPO excludePatterns;
    protected String msgType;

    /* Do not validate matcher */
    protected Pattern dnvp = new Pattern("Do not validate", REFlags.MULTILINE | REFlags.IGNORE_CASE);

    /** List of error messages */
    protected List<ErrMsgPO> errors = new ArrayList<ErrMsgPO>();

    /** File matcher */
    private FileMatcher fMatcher = new FileMatcher();

    /**
     * Check if changset must be skipped.
     * 
     * @param chgset
     * @return
     */
    protected boolean isDoNotValidate(String chgset) {
        Matcher m = dnvp.matcher(chgset);
        if (m.find()) {
            return true;
        }

        return false;
    }

    /**
     * Check if changset must be skipped.
     * 
     * @param chgset
     * @return
     */
    protected boolean isDoNotValidateComment(Node n) {
        NodeList kidz = n.getChildNodes();
        for (int i = 0; i < kidz.getLength(); i++) {
            if (kidz.item(i).getNodeType() == Element.COMMENT_NODE) {
                Comment c = (Comment) kidz.item(i);
                Matcher m = dnvp.matcher(c.getData());
                if (m.find()) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Set include and exclude paths.
     * 
     * @param includePathsMap
     * @param excludePathsMap
     */
    public void addPaths(Map<Integer, PatternPO> includePathsMap, Map<Integer, PatternPO> excludePathsMap) {
        /* Rule has its own paths */
        if (includePaths != null && !includePaths.isEmpty()) {
            List<PatternPO> includes = includePaths.getInPatternPO();
            for (PatternPO p : includes) {
                if (p.getPathPattern() == null) {
                    p.setPathPattern(includePathsMap.get(p.getId()).getPathPattern());
                }
            }
        }
        /* Get defined by default */
        else {
            IncludesPO inPaths = new IncludesPO();
            List<PatternPO> inPatterns = new ArrayList<PatternPO>();
            Set<Integer> keys = includePathsMap.keySet();
            for (Integer k : keys) {
                inPatterns.add(includePathsMap.get(k));
            }
            inPaths.setInPatternPO(inPatterns);
            setIncludePaths(inPaths);
        }

        /* Rule has its own paths */
        if (excludePatterns != null && !excludePatterns.isEmpty()) {
            List<PatternPO> excludes = excludePatterns.getExPatternPO();
            for (PatternPO p : excludes) {
                if (p.getPathPattern() == null) {
                    p.setPathPattern(excludePathsMap.get(p.getId()).getPathPattern());
                }
            }
        }
        /* Get defined by default */
        else {
            ExcludesPO exPaths = new ExcludesPO();
            List<PatternPO> exPatterns = new ArrayList<PatternPO>();
            Set<Integer> keys = excludePathsMap.keySet();
            for (Integer k : keys) {
                exPatterns.add(excludePathsMap.get(k));
            }
            exPaths.setExPatternPO(exPatterns);
            setExcludePatterns(exPaths);
        }
    }

    /**
     * Excute the rule.
     */
    public void executeRule() {
        /* Process include paths */
        MavenLoggerBridge.info("Executing rule '" + id + "'");
        try {
            processPath();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

    }

    /**
     * Check individual rule on file.
     * 
     * @param file
     * @param rule
     * @param whereArg
     *            - DELTA or CODE
     * @return The error message, if exist
     */
    protected ErrMsgPO checkRule(Path file) {
        ErrMsgPO errMsg = null;

        /* Run rule */
        String ret = runRule(line, file.toFile(), contentPattern);
        if (!ret.equalsIgnoreCase("")) {
            errMsg = new ErrMsgPO(id, file, this.line, ret, this.errMsg, this.msgType);
        }

        return errMsg;
    }

    /**
     * Run the rule
     * 
     * @param line2
     * @param file
     * @param contentPattern2
     * @return
     */
    protected String runRule(int line2, File file, String contentPattern2) {
        return null;
    }

    /**
     * Validate QG rules
     * 
     * @param file
     * @param type
     *            - SQL or XML
     * @param where
     *            - DELTA or CODE
     */
    protected void validateRules(Path file, String type, String where) {
        /* Rule does not apply to file extension */
        if (!this.getFileExt().equalsIgnoreCase(type)) {
            return;
        }

        /* Rule details */
        ErrMsgPO errMsg = checkRule(file);

        /* Add error if exists */
        if (errMsg != null) {
            errors.add(errMsg);
        }
    }

    /**
     * File is excluded.
     * 
     * @param file
     * @return
     */
    protected boolean isExcluded(Path file) {
        /* Exclusion patterns */
        if (excludePatterns != null) {
            PathMatcher pm = null;
            List<PatternPO> excluded = excludePatterns.getExPatternPO();
            for (PatternPO exc : excluded) {
                pm = FileSystems.getDefault().getPathMatcher("glob:" + exc.getPathPattern());
                /* Excluded */
                if (pm.matches(file)) {
                    return true;
                }
            }
        }

        /* Individual path pattern - Exclude paths if not matched */
        if (pathPattern != null) {
            PathMatcher pm = FileSystems.getDefault().getPathMatcher("glob:" + pathPattern);
            if (!pm.matches(file)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Process the file
     * 
     * @param file
     * @param attr
     * @return
     */
    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attr) {
        MavenLoggerBridge.debug("Process file: " + file);

        /* If excluded, continue */
        if (isExcluded(file)) {
            MavenLoggerBridge.debug("File excluded: " + file);
            return FileVisitResult.CONTINUE;
        }

        /* Match file extension */
        String ext = "";
        if (fMatcher.matchSQLFile(file)) {
            MavenLoggerBridge.debug("File is SQL: " + file);
            ext = "sql";
        } else if (fMatcher.matchXMLFile(file)) {
            MavenLoggerBridge.debug("File is XML: " + file);
            ext = "xml";
        } else {
            MavenLoggerBridge.debug("File do not match by extension: " + file);
        }

        /* Match by type (DELTA or CODE) */
        String where = "";
        if (fMatcher.matchCODEFile(file) || fMatcher.matchRollbackFile(file)) {
            MavenLoggerBridge.debug("File is CODE or ROLLBACK: " + file);
            where = "CODE";
        } else if (fMatcher.matchDELTAFile(file)) {
            MavenLoggerBridge.debug("File is DELTA: " + file);
            where = "DELTA";
        }

        /* Validate rules for current file */
        validateRules(file, ext, where);

        return FileVisitResult.CONTINUE;
    }

    /**
     * Process artifact content.
     * 
     * @throws IOException
     */
    protected void processPath() throws IOException {
        /* Include paths */
        if (includePaths != null) {
            List<PatternPO> paths = includePaths.getInPatternPO();
            /* For each include path... */
            for (PatternPO path : paths) {
                MavenLoggerBridge.info("Process path: " + path);
                Files.walkFileTree(FileSystems.getDefault().getPath(path.getPathPattern()), this);
            }

            /* Print error messages */
            printMessages();
        }
    }

    /**
     * Print and process rules messages.
     */
    protected void printMessages() {
        /* Print error messages */
        if (errors != null) {
            for (ErrMsgPO error : errors) {
                /* Warning */
                if (msgType.equalsIgnoreCase(MsgType.WARNING.getId())) {
                    MavenLoggerBridge.warn(error.toString());
                }
                /* Error */
                else if (msgType.equalsIgnoreCase(MsgType.ERROR.getId())) {
                    MavenLoggerBridge.error(error.toString());
                }
                /* Fatal */
                else if (msgType.equalsIgnoreCase(MsgType.FATAL.getId())) {
                    MavenLoggerBridge.error(error.toString());
                    return;
                }
            }
        }
    }

    /**
     * Check if rule is FATAL.
     * 
     * @return True or false
     */
    public boolean isFatal() {
        if (msgType.equalsIgnoreCase(MsgType.FATAL.getId())) {
            return true;
        }

        return false;
    }

    /**
     * @return the line
     */
    public int getLine() {
        return line;
    }

    /**
     * @param line
     *            the line to set
     */
    public void setLine(int line) {
        this.line = line;
    }

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
    public String getId() {
        return id;
    }

    /**
     * @param id
     *            the id to set
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return the fileExt
     */
    public String getFileExt() {
        return fileExt;
    }

    /**
     * @param fileExt
     *            the fileExt to set
     */
    public void setFileExt(String fileExt) {
        this.fileExt = fileExt;
    }

    /**
     * @return the contentPattern
     */
    public String getContentPattern() {
        return contentPattern;
    }

    /**
     * @param contentPattern
     *            the contentPattern to set
     */
    public void setContentPattern(String contentPattern) {
        this.contentPattern = contentPattern;
    }

    /**
     * @return the errMsg
     */
    public String getErrMsg() {
        return errMsg;
    }

    /**
     * @param errMsg
     *            the errMsg to set
     */
    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
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
    }

    /**
     * Get error messages
     * 
     * @return
     */
    public List<ErrMsgPO> getErrorMessages() {
        return this.errors;
    }

    /**
     * @return the msgType
     */
    public String getMsgType() {
        return msgType;
    }

    /**
     * @param msgType
     *            the msgType to set
     */
    public void setMsgType(String msgType) {
        this.msgType = msgType;
    }

    /**
     * @return the attrPattern
     */
    public String getAttrPattern() {
        return attrPattern;
    }

    /**
     * @param attrPattern
     *            the attrPattern to set
     */
    public void setAttrPattern(String attrPattern) {
        this.attrPattern = attrPattern;
    }

    /**
     * To string.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Id:" + this.id + "; FileExt:" + this.fileExt + "; Line:" + this.line + "; ContPattern:"
                + this.contentPattern + "; ErrMsg:" + this.errMsg + "\n");
        if (includePaths != null) {
            sb.append("Include patterns:" + this.includePaths.toString());
            sb.append("\n");
        }
        if (excludePatterns != null) {
            sb.append("Exclude patterns:" + this.excludePatterns.toString());
            sb.append("\n");
        }
        return sb.toString();
    }
}
