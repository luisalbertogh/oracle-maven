/**
 * Check changesets ids. Validate that composition is correct.
 */
package com.db.volcker.sdlc.oracle.rules;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import jregex.MatchIterator;
import jregex.MatchResult;
import jregex.Matcher;
import jregex.Pattern;
import jregex.REFlags;

/**
 * @author garcluia
 *
 */
public final class ChgsetIdRule extends AbstractRule implements RuleInterface {

    /** List of current jira ids within the same file */
    private List<String> jiraIds;

    /** Check chgset id content */
    private boolean checkContent = true;

    /**
     * Rule implementation.
     * 
     * @param nline
     * @param file
     * @param pattern
     * @param type
     * @return
     */
    @Override
    public String runRule(int nline, File file, String contentPattern) {
        /* Init jira list */
        jiraIds = new ArrayList<String>();

        /* Read file content */
        String fullFile = "";
        try (FileInputStream fis = new FileInputStream(file)) {
            byte[] data = new byte[(int) file.length()];
            fis.read(data);
            fullFile = new String(data, "UTF-8");
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        /* Return string */
        String ret = "";

        /* Changeset id pattern */
        Pattern chgsetidpattern = new Pattern(contentPattern, REFlags.MULTILINE | REFlags.IGNORE_CASE);

        /* Find changesets */
        Pattern p = new Pattern("^--changeset.*$", REFlags.MULTILINE | REFlags.IGNORE_CASE);
        Matcher m = p.matcher(fullFile);
        MatchIterator mi = m.findAll();
        while (mi.hasMore()) {
            MatchResult mr = mi.nextMatch();
            String chgsetid = mr.toString().trim();

            /* Skip 'do not validate' */
            if (isDoNotValidate(chgsetid)) {
                continue;
            }

            /* Validate changesets format */
            ret += validateFormat(nline, file, contentPattern, chgsetid, chgsetidpattern);

            /* Validate changsets composition */
            ret += validateName(nline, file, contentPattern, chgsetid, chgsetidpattern);
        }

        return ret;
    }

    /**
     * Validate changeset id format.
     * 
     * @param nline
     * @param file
     * @param contentPattern
     * @param chgset
     * @param chgsetidpattern
     * @return The wrong changesets
     */
    private String validateFormat(int nline, File file, String contentPattern, String chgset, Pattern chgsetidpattern) {
        String ret = "";

        /* Check if chgsetidpattern match pattern for each changeset */
        Matcher m2 = chgsetidpattern.matcher(chgset);
        if (!m2.find()) {
            ret += chgset + "\n";
        }

        return ret;
    }

    /**
     * Validate that changeset id is composed by the right fix version and jira
     * id.
     * 
     * @param nline
     * @param file
     * @param contentPattern
     * @param chgset
     * @param chgsetidpattern
     * @return The wrong changesets
     */
    private String validateName(int nline, File file, String contentPattern, String chgset, Pattern chgsetidpattern) {
        /* Return string */
        String ret = "";

        /* Changeset ID pattern */
        Matcher m = chgsetidpattern.matcher(chgset);
        MatchIterator mi = m.findAll();
        /* Find changeset ids */
        while (mi.hasMore()) {
            MatchResult mr = mi.nextMatch();
            String chgsetid = mr.toString().trim();
            /* Split in tokens */
            String fixVersion = chgsetid.split(":")[0];
            String jiraIdRn = chgsetid.split(":")[1];

            /* Check if already present */
            boolean duplicated = isDuplicate(jiraIdRn);

            String[] jiraIdTokens = jiraIdRn.split("-");
            String jiraId = jiraIdTokens[0] + "-" + jiraIdTokens[1];

            /* Iterate over the file path */
            boolean fixVersionOk = false;
            boolean jiraIdOk = false;
            Iterator<Path> itPath = file.getParentFile().toPath().iterator();
            while (itPath.hasNext()) {
                Path path = itPath.next();
                String pname = path.getFileName().toString();
                /* Check that fixVersion is within the path */
                if (checkContent && !fixVersionOk && fixVersion.equalsIgnoreCase(pname)) {
                    fixVersionOk = true;
                }
                /* Check that jiraId is within the path */
                if (!jiraIdOk && jiraId.equalsIgnoreCase(pname)) {
                    jiraIdOk = true;
                }

                if (fixVersionOk && jiraIdOk) {
                    break;
                }
            }

            /* Set as wrong if not correct */
            if ((checkContent && !fixVersionOk) || !jiraIdOk || duplicated) {
                ret += "--changeset " + chgsetid + "\n";
            }
        }

        return ret;
    }

    /**
     * Is the jira id already included within the same file?
     * 
     * @param jiraId
     *            - The jiraid
     * @return True or false
     */
    boolean isDuplicate(String jiraId) {
        if (jiraIds.contains(jiraId)) {
            return true;
        }

        jiraIds.add(jiraId);
        return false;
    }

    /**
     * @return the checkContent
     */
    public boolean isCheckContent() {
        return checkContent;
    }

    /**
     * @param checkContent
     *            the checkContent to set
     */
    public void setCheckContent(boolean checkContent) {
        this.checkContent = checkContent;
    }

}
