/**
 * Check that content exists in a concrete line.
 */
package com.db.volcker.sdlc.oracle.rules;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.db.volcker.sdlc.oracle.utils.MavenLoggerBridge;

import jregex.Matcher;
import jregex.Pattern;
import jregex.REFlags;

/**
 * @author garcluia
 *
 */
public final class XmlChangeSetRule extends AbstractRule implements RuleInterface {

    /** Changeset ID pattern */
    private String idPattern;

    /** List of current jira ids within the same file */
    private List<String> jiraIds;

    /**
     * Possible replacement between pattern and attribute (to make it
     * non-case-sensitive, i.e.)
     */
    private static Map<String, String> replacements = new HashMap<String, String>();

    /* Load replacements */
    static {
        replacements.put("runonchange", "runOnChange");
        replacements.put("runalways", "runAlways");
        replacements.put("failonerror", "failOnError");
    }

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

        /* Init jira list */
        jiraIds = new ArrayList<String>();

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
                    /* Changeset */
                    Node n = nl.item(i);

                    /* Skip do not validate changesets */
                    if (isDoNotValidateComment(n)) {
                        continue;
                    }

                    /* Changeset id & author */
                    String chgsetId = n.getAttributes().getNamedItem("id").getNodeValue();
                    String author = n.getAttributes().getNamedItem("author").getNodeValue();

                    /* Attribute pattern */
                    String attrPattern = getAttrPattern();
                    String[] attrTokens = attrPattern.split("\\|\\|");
                    for (String attr : attrTokens) {
                        String id = attr.split(":")[0];
                        /* Apply replacement if needed */
                        if (replacements.get(id) != null) {
                            id = replacements.get(id);
                        }

                        /* Validate attribute up to pattern */
                        boolean valid = validateAttribute(n.getAttributes().getNamedItem(id), attr);
                        if (!valid) {
                            ret += "\nIn " + file.getAbsolutePath() + " - Chgset ID: " + chgsetId + " - Attribute " + id
                                    + " has an invalid value or does not exist";
                        }
                    }

                    /* Validate rollback */
                    if (!validateRollback(n, file)) {
                        ret += "\nIn " + file.getAbsolutePath() + " - Chgset ID: " + chgsetId
                                + " - Rollback is not valid";
                    }

                    /* Validate ID */
                    if (!validateChgsetId(author, chgsetId, file)) {
                        ret += "\nIn " + file.getAbsolutePath() + " - Chgset ID: " + chgsetId
                                + " - ID and/or author is not valid or it is duplicated";
                    }
                }
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        return ret;
    }

    /**
     * Validate changeset id (id + author)
     * 
     * @param chgsetId
     *            - The changeset id
     * @param file
     *            - The file
     * 
     * @return True or false
     */
    private boolean validateChgsetId(String author, String id, File file) {
        /* Validate format */
        Pattern p = new Pattern(idPattern, REFlags.MULTILINE | REFlags.IGNORE_CASE);
        Matcher m = p.matcher(author + ":" + id);
        if (!m.find()) {
            return false;
        }

        /* Validate content */
        String[] t = id.split("-");
        String newid = t[0] + "-" + t[1];
        if (!validatePath(file, author, newid)) {
            return false;
        }

        /* Check if already present */
        if (isDuplicate(id)) {
            return false;
        }

        return true;
    }

    /**
     * Validate that rollback is present. Check that rollback file is included
     * in right file path.
     * 
     * @param chgset
     *            - XML changeset node
     * @param file
     *            - The current file
     * @return True or false
     */
    private boolean validateRollback(Node chgset, File file) {
        NodeList kidz = chgset.getChildNodes();
        for (int i = 0; i < kidz.getLength(); i++) {
            Node n = kidz.item(i);
            String name = n.getNodeName();
            /* Rollback node */
            if (name.equalsIgnoreCase("rollback")) {
                /* Rollback sql file */
                NodeList rbKidz = n.getChildNodes();
                for (int j = 0; j < rbKidz.getLength(); j++) {
                    Node rbn = rbKidz.item(j);
                    String rbname = rbn.getNodeName();
                    if (rbname.equalsIgnoreCase("sqlfile")) {
                        String rbpath = rbn.getAttributes().getNamedItem("path").getNodeValue();
                        String[] tokens = rbpath.split("/");
                        String fixVersion = tokens[1];
                        String jiraId = tokens[2];

                        /* Validate sql filepath */
                        return validatePath(file, fixVersion, jiraId);
                    }
                }
            }
        }

        return false;
    }

    /**
     * Validate attribute value with set pattern.
     * 
     * @param attrNode
     *            - The attribute xml value
     * @param attrPattern
     *            - The attribute desired values
     * @return True or false
     */
    private boolean validateAttribute(Node attrNode, String attrPattern) {
        /* Attr pattern */
        String[] pTokens = attrPattern.split(":");
        String value = pTokens[1];
        String optional = null;
        if (pTokens.length > 2) {
            optional = pTokens[2];
        }

        /* If mandatory and does not exist, return false */
        if (optional == null && attrNode == null) {
            return false;
        }

        /* Attribute exists */
        if (attrNode != null) {
            String nValue = attrNode.getNodeValue();
            /* Different value, return false */
            if (!nValue.equalsIgnoreCase(value)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Validate that changeset id is composed by the right fix version and jira
     * id.
     * 
     * @param file
     * @param fixVersion
     * @param jiraId
     * @return True or false
     * 
     */
    private boolean validatePath(File file, String fixVersion, String jiraId) {
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
            return false;
        }

        return true;
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
     * @return the idPattern
     */
    public String getIdPattern() {
        return idPattern;
    }

    /**
     * @param idPattern
     *            the idPattern to set
     */
    public void setIdPattern(String idPattern) {
        this.idPattern = idPattern;
    }
}
