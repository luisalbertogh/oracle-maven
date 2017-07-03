/**
 * Maven plugin.
 */
package com.db.volcker.sdlc.oracle.plugin;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.beanutils.BeanDeclaration;
import org.apache.commons.configuration2.beanutils.BeanHelper;
import org.apache.commons.configuration2.beanutils.XMLBeanDeclaration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.db.volcker.sdlc.oracle.domain.ConfPO;
import com.db.volcker.sdlc.oracle.domain.ErrMsgPO;
import com.db.volcker.sdlc.oracle.domain.RulesPO;
import com.db.volcker.sdlc.oracle.rules.RuleInterface;
import com.db.volcker.sdlc.oracle.utils.MavenLoggerBridge;

/**
 * @author garcluia
 *
 */
@Mojo(name = "validate", requiresDependencyResolution = ResolutionScope.NONE, defaultPhase = LifecyclePhase.NONE, threadSafe = true, aggregator = true)
public class OracleMavenPlugin extends AbstractMojo {
    /** Log4j logger */
    private static Logger logger = LoggerFactory.getLogger(OracleMavenPlugin.class);

    /** Config file */
    @Parameter(defaultValue = "quality-gate-config.xml", property = "confFile")
    private String confFile = "quality-gate-config.xml";

    /** The configuration bean */
    private ConfPO confBean;

    /** List of error messages */
    private List<ErrMsgPO> errors = new ArrayList<ErrMsgPO>();

    /**
     * Execute the Mojo.
     */
    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        /* Enable maven log if log4j not enable */
        if (!MavenLoggerBridge.isLog4jOn()) {
            MavenLoggerBridge.setMavenLogger(getLog(), true, false);
        }

        /* Load config file */
        MavenLoggerBridge.info("Loading config file: " + confFile);
        try {
            loadConfig(confFile);
        } catch (ConfigurationException ex) {
            throw new MojoExecutionException(ex.getMessage());
        }

        MavenLoggerBridge.info("Configuration loaded.");

        /* Apply rules */
        MavenLoggerBridge.info("Start processing rules...");
        RulesPO rulesPO = confBean.getRules();
        if (rulesPO != null) {
            List<RuleInterface> rules = rulesPO.getRule();
            for (RuleInterface rule : rules) {
                /* Add paths to rule */
                rule.addPaths(confBean.getIncludePathsMap(), confBean.getExcludePathsMap());

                /* Execute rule */
                rule.executeRule();

                /* Add rule error messages */
                if (rule.getErrorMessages() != null && rule.getErrorMessages().size() > 0) {
                    errors.addAll(rule.getErrorMessages());

                    /* Check if rule is fatal */
                    if (rule.isFatal()) {
                        break;
                    }
                }
            }
        }

        /* If errors, throw a MojoFailureException */
        if (errors != null && errors.size() > 0) {
            for (ErrMsgPO error : errors) {
                /* If single error or fatal found, throw failure exception */
                if (error.getType().equalsIgnoreCase("error") || error.getType().equalsIgnoreCase("fatal")) {
                    throw new MojoFailureException(errors.size() + " errors have been found!!!");
                }
            }
        }
    }

    /**
     * Load config file.
     * 
     * @throws ConfigurationException
     */
    private void loadConfig(String confFile) throws ConfigurationException {
        /* Add rules */
        Parameters params = new Parameters();
        FileBasedConfigurationBuilder<XMLConfiguration> builder = new FileBasedConfigurationBuilder<XMLConfiguration>(
                XMLConfiguration.class).configure(params.xml().setFileName(confFile));
        XMLConfiguration config = builder.getConfiguration();
        BeanDeclaration decl = new XMLBeanDeclaration(config, "confBean");
        confBean = (ConfPO) BeanHelper.INSTANCE.createBean(decl);

        /* Print configuration */
        printConfiguration(confBean);
    }

    /**
     * Print loaded configuration.
     */
    public void printConfiguration(ConfPO config) {
        logger.debug(config.toString());
    }

    /**
     * @return the confFile
     */
    public String getConfFile() {
        return confFile;
    }

    /**
     * @param confFile
     *            the confFile to set
     */
    public void setConfFile(String confFile) {
        this.confFile = confFile;
    }
}
