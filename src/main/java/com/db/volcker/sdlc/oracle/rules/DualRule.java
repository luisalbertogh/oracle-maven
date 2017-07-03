/**
 * Check that if first pattern exists, the second must be present either.
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
public final class DualRule extends AbstractRule implements RuleInterface {
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
        String[] patterns = contentPattern.split("-->");
        Pattern p1 = new Pattern(patterns[0], REFlags.MULTILINE | REFlags.IGNORE_CASE);
        Pattern p2 = new Pattern(patterns[1], REFlags.MULTILINE | REFlags.IGNORE_CASE);

        /* Iterate over changesets */
        String ret = "";
        for (String chgset : changesets) {
            /* Skip 'do not validate' */
            if (isDoNotValidate(chgset)) {
                continue;
            }

            /* Check if p1 matches */
            Matcher m1 = p1.matcher(chgset);
            if (m1.find()) {
                /* Then m2 has to match as well */
                Matcher m2 = p2.matcher(chgset);
                if (!m2.find()) {
                    ret += "--changset" + chgset.split("\n")[0] + "\n";
                }
            }
        }

        return ret;
    }

}
