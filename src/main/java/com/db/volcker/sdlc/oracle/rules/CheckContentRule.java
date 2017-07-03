/**
 * Check that content exists in a concrete line.
 */
package com.db.volcker.sdlc.oracle.rules;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import jregex.Matcher;
import jregex.Pattern;
import jregex.REFlags;

/**
 * @author garcluia
 *
 */
public final class CheckContentRule extends AbstractRule implements RuleInterface {
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
        /* Set pattern */
        Pattern p = new Pattern(contentPattern, REFlags.MULTILINE | REFlags.IGNORE_CASE);

        try (BufferedReader bf = new BufferedReader(new FileReader(file))) {
            int cont = 0;
            String line = null;
            while ((line = bf.readLine()) != null) {
                cont++;
                if (cont == nline) {
                    /* Pattern matcher */
                    Matcher m = p.matcher(line);

                    /* Line does not match pattern */
                    if (!m.matches()) {
                        return line;
                    }

                    return "";
                }
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        return "";
    }
}
