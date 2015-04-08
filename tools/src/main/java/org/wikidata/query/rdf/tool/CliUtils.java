package org.wikidata.query.rdf.tool;

import static com.google.common.io.Resources.getResource;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.Reader;
import java.io.Writer;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.URI;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.uri.WikibaseUris;
import org.wikidata.query.rdf.tool.rdf.Munger;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;

import com.google.common.base.Charsets;
import com.lexicalscope.jewel.cli.ArgumentValidationException;
import com.lexicalscope.jewel.cli.Cli;
import com.lexicalscope.jewel.cli.CliFactory;
import com.lexicalscope.jewel.cli.HelpRequestedException;
import com.lexicalscope.jewel.cli.Option;

/**
 * Utilities for command line scripts.
 */
public class CliUtils {
    private static final Logger log = LoggerFactory.getLogger(Update.class);

    /**
     * Basic options for parsing with JewelCLI.
     */
    public interface BasicOptions {
        @Option(shortName = "v", description = "Verbose mode")
        boolean verbose();

        @Option(helpRequest = true, description = "Show this message")
        boolean help();
    }

    /**
     * Command line options for setting up a Munger instance.
     */
    public interface MungerOptions extends WikibaseOptions {
        @Option(longName = "labelLanguage", defaultToNull = true, description = "Only import labels, aliases, and descriptions in these languages.")
        List<String> labelLanguages();

        @Option(longName = "singleLabel", defaultToNull = true, description = "Always import a single label and description using the languages specified as a fallback list.  If there aren't any matching labels or descriptions them the entity itself is used as the label or description.")
        List<String> singleLabelLanguages();

        @Option(description = "Skip site links")
        boolean skipSiteLinks();
    }

    /**
     * Options specifying information about a Wikibase instance.
     */
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
     * Build a reader for the uri.
     */
    public static Reader reader(String uri) throws IOException {
        return new InputStreamReader(inputStream(uri), Charsets.UTF_8);
    }

    /**
     * Get an input stream for a uri. If the uri looks like a gzip file then
     * unzips it on the fly.
     */
    public static InputStream inputStream(String uri) throws IOException {
        if (uri.equals("-")) {
            return ForbiddenOk.systemDotIn();
        }
        InputStream stream;
        if (!uri.contains(":/")) {
            stream = new BufferedInputStream(new FileInputStream(uri));
        } else {
            stream = URI.create(uri).toURL().openStream();
        }
        if (uri.endsWith(".gz")) {
            stream = new GZIPInputStream(stream);
        }
        return stream;
    }

    /**
     * Build a writer for the uri.
     */
    public static Writer writer(String uri) throws IOException {
        return new OutputStreamWriter(outputStream(uri), Charsets.UTF_8);
    }

    /**
     * Get an output stream for a uri. If the uri looks like a gzip file then
     * zips it on the fly.
     */
    public static OutputStream outputStream(String out) throws IOException {
        if (out.equals("-")) {
            return ForbiddenOk.systemDotOut();
        }
        OutputStream stream = new BufferedOutputStream(new FileOutputStream(out));
        if (out.endsWith(".gz")) {
            stream = new GZIPOutputStream(stream);
        }
        return stream;
    }

    public static UncaughtExceptionHandler loggingUncaughtExceptionHandler(final String message, final Logger log) {
        return new UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread thread, Throwable exception) {
                log.error(message, exception);
            }
        };
    }

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

    /**
     * Methods in this class are ignored by the forbiddenapis checks. Thus you
     * need to really really really be sure what you are putting in here is
     * right.
     */
    private static class ForbiddenOk {
        /**
         * Get System.in. CliTools should be allowed to use System.in/out/err.
         * This is private because we only want them to be used by cli tools.
         */
        private static InputStream systemDotIn() {
            return System.in;
        }

        private static PrintStream systemDotOut() {
            return System.out;
        }

        private static PrintStream systemDotErr() {
            return System.err;
        }
    }
}
