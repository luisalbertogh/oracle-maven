/**
 * 
 */
package com.db.volcker.sdlc.oracle.utils;

import org.apache.maven.plugin.logging.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.db.volcker.sdlc.oracle.plugin.OracleMavenPlugin;

/**
 * @author garcluia
 *
 */
public final class MavenLoggerBridge {
    /** Maven logger */
    private static Log loggerMaven;

    /** Log4j logger */
    private static Logger logger4j = LoggerFactory.getLogger(OracleMavenPlugin.class);

    /** Maven enable */
    private static boolean mavenEnable = true;

    /** Log4j enable */
    private static boolean log4jEnable = false;

    /**
     * Set mavne logger.
     * 
     * @param loggerArg
     */
    public static void setMavenLogger(Log loggerArg, boolean mavenEnableArg, boolean log4jEnableArg) {
        loggerMaven = loggerArg;
        mavenEnable = mavenEnableArg;
        log4jEnable = log4jEnableArg;
    }

    /**
     * Is logback on?
     * 
     * @param msg
     * @return
     */
    public static boolean isLog4jOn() {
        return log4jEnable;
    }

    /**
     * DEBUG
     * 
     * @param msg
     */
    public static void debug(String msg) {
        append(msg, "DEBUG");
    }

    /**
     * INFO
     * 
     * @param msg
     */
    public static void info(String msg) {
        append(msg, "INFO");
    }

    /**
     * WARN
     * 
     * @param msg
     */
    public static void warn(String msg) {
        append(msg, "WARN");
    }

    /**
     * ERROR
     * 
     * @param msg
     */
    public static void error(String msg) {
        append(msg, "ERROR");
    }

    /** Append log */
    public static void append(String msg, String level) {
        if (mavenEnable) {
            appendMaven(msg, level);
        }

        if (log4jEnable) {
            appendLog4j(msg, level);
        }
    }

    /** Append log */
    public static void appendMaven(String msg, String level) {
        if (level.equalsIgnoreCase("DEBUG") || level.equalsIgnoreCase("TRACE")) {
            loggerMaven.debug(msg);
        } else if (level.equalsIgnoreCase("INFO")) {
            loggerMaven.info(msg);
        } else if (level.equalsIgnoreCase("WARN")) {
            loggerMaven.warn(msg);
        } else if (level.equalsIgnoreCase("ERROR")) {
            loggerMaven.error(msg);
        }
    }

    /** Append log4j log */
    public static void appendLog4j(String msg, String level) {
        if (level.equalsIgnoreCase("DEBUG") || level.equalsIgnoreCase("TRACE")) {
            logger4j.debug(msg);
        } else if (level.equalsIgnoreCase("INFO")) {
            logger4j.info(msg);
        } else if (level.equalsIgnoreCase("WARN")) {
            logger4j.warn(msg);
        } else if (level.equalsIgnoreCase("ERROR")) {
            logger4j.error(msg);
        }
    }
}
