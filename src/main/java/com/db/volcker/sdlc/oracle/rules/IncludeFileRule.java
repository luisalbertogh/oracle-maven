/**
 * Check that content exists in a concrete line.
 */
package com.db.volcker.sdlc.oracle.rules;

import java.io.File;
import java.nio.file.Path;
import java.util.Iterator;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.db.volcker.sdlc.oracle.utils.MavenLoggerBridge;

/**
 * @author garcluia
 *
 */
public final class IncludeFileRule extends AbstractRule implements RuleInterface {
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
        String ret = "";

        /* Log */
        MavenLoggerBridge.debug("Processing file: " + file.getAbsolutePath());

        try {
            /* Get XML node and attribute value */
            XPath xpath = XPathFactory.newInstance().newXPath();
            NodeList nl = (NodeList) xpath.evaluate(contentPattern, new InputSource(file.getAbsolutePath()),
                    XPathConstants.NODESET);

            /* If not empty */
            if (nl.getLength() > 0) {
                for (int i = 0; i < nl.getLength(); i++) {
                    Node n = nl.item(i);
                    String value = n.getNodeValue();
                    MavenLoggerBridge.debug("Value: " + value);

                    /* Vaidate include value */
                    ret += validateIncludeValue(value, file);
                }
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        return ret;
    }

    /**
     * Validate string value with file path.
     * 
     * @param value
     * @param file
     * @return
     */
    private String validateIncludeValue(String value, File file) {
        String ret = "";
        String[] tokens = value.split("/");
        String fixVersion = tokens[1];
        String jiraId = tokens[2];

        /* Iterate over the file path */
        boolean fixVersionOk = false;
        boolean jiraIdOk = false;
        Iterator<Path> itPath = file.getParentFile().toPath().iterator();
        while (itPath.hasNext()) {
            Path path = itPath.next();
            String pname = path.getFileName().toString();
            /* Check that fixVersion is within the path */
            if (!fixVersionOk && fixVersion.equalsIgnoreCase(pname)) {
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
        if (!fixVersionOk || !jiraIdOk) {
            ret += "include file='" + value
                    + "' seems to be pointing to a wrong file (different FIX VERSION and/or JIRA ID).\n";
        }

        return ret;
    }
}
