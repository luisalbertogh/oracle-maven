/**
 * Main class.
 */
package com.db.volcker.sdlc.oracle;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;

import com.db.volcker.sdlc.oracle.plugin.OracleMavenPlugin;
import com.db.volcker.sdlc.oracle.utils.MavenLoggerBridge;

/**
 * @author garcluia
 *
 */
public class OracleValidator {

    /**
     * @param args
     */
    public static void main(String[] args) {
        /* Enable logback log system */
        MavenLoggerBridge.setMavenLogger(null, false, true);

        /* Optional arguments */
        String configFile = null;
        if (args.length > 0) {
            /* config file */
            configFile = args[0];
        }

        OracleMavenPlugin omp = new OracleMavenPlugin();
        try {
            /* Execute plugin */
            if (configFile != null) {
                omp.setConfFile(configFile);
            }
            omp.execute();
        } catch (MojoExecutionException | MojoFailureException ex) {
            ex.printStackTrace();
        }
    }
}
