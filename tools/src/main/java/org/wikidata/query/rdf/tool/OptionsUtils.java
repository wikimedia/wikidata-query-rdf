package org.wikidata.query.rdf.tool;

import static com.google.common.io.Resources.getResource;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.uri.WikibaseUris;
import org.wikidata.query.rdf.tool.CliUtils.ForbiddenOk;
import org.wikidata.query.rdf.tool.rdf.Munger;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;

import com.lexicalscope.jewel.cli.ArgumentValidationException;
import com.lexicalscope.jewel.cli.Cli;
import com.lexicalscope.jewel.cli.CliFactory;
import com.lexicalscope.jewel.cli.HelpRequestedException;
import com.lexicalscope.jewel.cli.Option;

/**
 * Utilities for parsing options.
 */
public final class OptionsUtils {
    private static final Logger log = LoggerFactory.getLogger(OptionsUtils.class);

    /**
     * Basic options for parsing with JewelCLI.
     */
    @SuppressWarnings("checkstyle:javadocmethod")
    public interface BasicOptions {
        @Option(shortName = "v", description = "Verbose mode")
        boolean verbose();

        @Option(helpRequest = true, description = "Show this message")
        boolean help();
    }

    /**
     * Command line options for setting up a Munger instance.
     */
    @SuppressWarnings("checkstyle:javadocmethod")
    public interface MungerOptions extends WikibaseOptions {
        @Option(longName = "labelLanguage", defaultToNull = true, description = "Only import labels, aliases, and descriptions in these languages.")
        List<String> labelLanguages();

        @Option(longName = "singleLabel", defaultToNull = true, description = "Only import a single label and description using the languages "
                + "specified as a fallback list. If there isn't a label in any of the specified languages then no label is imported.  Ditto for "
                + "description.")
        List<String> singleLabelLanguages();

        @Option(description = "Skip site links")
        boolean skipSiteLinks();
    }

    /**
     * Options specifying information about a Wikibase instance.
     */
    @SuppressWarnings("checkstyle:javadocmethod")
    public interface WikibaseOptions {
        @Option(shortName = "w", defaultValue = "www.wikidata.org", description = "Wikibase host")
        String wikibaseHost();
    }

    /**
     * Parses options and handles the verbose flag.
     *
     * @param optionsClass class defining options
     * @param args arguments to parse
     * @return parse options
     */
    public static <T extends BasicOptions> T handleOptions(Class<T> optionsClass, String... args) {
        T options = parseOptions(optionsClass, args);
        if (options.verbose()) {
            log.info("Verbose mode activated");
            // Assumes logback which is pretty safe in main.
            LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
            try {
                JoranConfigurator configurator = new JoranConfigurator();
                configurator.setContext(context);
                context.reset();
                configurator.doConfigure(getResource("logback-verbose.xml"));
            } catch (JoranException je) {
                // StatusPrinter will handle this
            }
            StatusPrinter.printInCaseOfErrorsOrWarnings(context);
        }
        return options;
    }

    /**
     * Build a munger from a MungerOptions instance.
     */
    public static Munger mungerFromOptions(MungerOptions options) {
        Munger munger = new Munger(new WikibaseUris(options.wikibaseHost()));
        if (options.skipSiteLinks()) {
            munger = munger.removeSiteLinks();
        }
        if (options.labelLanguages() != null) {
            munger = munger.limitLabelLanguages(options.labelLanguages());
        }
        if (options.singleLabelLanguages() != null) {
            munger = munger.singleLabelMode(options.singleLabelLanguages());
        }
        return munger;
    }

    /**
     * Parse command line options, exiting if there is an error or the user
     * asked for help.
     */
    private static <T> T parseOptions(Class<T> optionsClass, String... args) {
        Cli<T> cli = CliFactory.createCli(optionsClass);
        try {
            return cli.parseArguments(args);
        } catch (HelpRequestedException e) {
            ForbiddenOk.systemDotOut().println(cli.getHelpMessage());
            System.exit(0);
        } catch (ArgumentValidationException e) {
            ForbiddenOk.systemDotErr().println("Invalid argument:  " + e);
            ForbiddenOk.systemDotErr().println(cli.getHelpMessage());
            System.exit(1);
        }
        throw new RuntimeException("Should be unreachable.");
    }

    private OptionsUtils() {
        // Utils uncallable constructor
    }
}
