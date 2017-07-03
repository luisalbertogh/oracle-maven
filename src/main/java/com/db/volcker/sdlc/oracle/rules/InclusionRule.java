/**
 * Validate if a pattern is included or excluded.
 */
package com.db.volcker.sdlc.oracle.rules;

import java.io.File;
import java.io.FileInputStream;

import jregex.Matcher;
import jregex.Pattern;
import jregex.REFlags;

/**
 * @author garcluia
 *
 */
public final class InclusionRule extends AbstractRule implements RuleInterface {
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
        /* Read file content */
        String fullFile = "";
        try (FileInputStream fis = new FileInputStream(file)) {
            byte[] data = new byte[(int) file.length()];
            fis.read(data);
            fullFile = new String(data, "UTF-8");
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        /* Split changesets */
        String[] changesets = fullFile.split("--changeset");

        /* Patterns */
        String[] pattTokens = contentPattern.split("\\|\\|");
        String operator = pattTokens[0];
        Pattern p1 = new Pattern(pattTokens[1], REFlags.MULTILINE | REFlags.IGNORE_CASE);

        /* Iterate over changesets */
        String ret = "";
        for (String chgset : changesets) {
            /* Skip first SQL line */
            chgset = chgset.trim();
            if (chgset.indexOf("--liquibase formatted sql") != -1) {
                continue;
            }

            /* Skip 'do not validate' */
            if (isDoNotValidate(chgset)) {
                continue;
            }

            /* Matcher for this changeset */
            Matcher m1 = p1.matcher(chgset);

            /* If operator is '+', it has to be included */
            if (operator.equalsIgnoreCase("+") && !m1.find()) {
                ret += "--changset " + chgset.split("\n")[0] + "\n";
            } else if (operator.equalsIgnoreCase("-") && m1.find()) {
                ret += "--changset " + chgset.split("\n")[0] + "\n";
            }
        }

        return ret;
    }

}
