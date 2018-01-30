package org.wikidata.query.rdf.tool.options;

import java.util.List;

import com.lexicalscope.jewel.cli.Option;

/**
 * CLI options for use with JewelCli.
 */
@SuppressWarnings("checkstyle:javadocmethod")
public interface UpdateOptions extends OptionsUtils.BasicOptions, OptionsUtils.MungerOptions, OptionsUtils.WikibaseOptions {
    @Option(defaultValue = "https", description = "Wikidata url scheme")
    String wikibaseScheme();

    @Option(shortName = "s", defaultToNull = true, description = "Start time in 2015-02-11T17:11:08Z or 20150211170100 format.")
    String start();

    @Option(shortName = "I", longName = "init", description = "Initialize last update time to start time")
    boolean init();

    @Option(defaultToNull = true, description = "If specified must be <id> or list of <id>, comma or space separated.")
    List<String> ids();

    @Option(defaultToNull = true, description = "If specified must be <start>-<end>. Ids are iterated instead of recent "
            + "changes. Start and end are inclusive.")
    String idrange();

    @Option(shortName = "u", description = "URL to post updates and queries.")
    String sparqlUrl();

    @Option(shortName = "d", defaultValue = "10", description = "Poll delay when no updates found")
    int pollDelay();

    @Option(shortName = "t", defaultValue = "10", description = "Thread count")
    int threadCount();

    @Option(shortName = "b", defaultValue = "100", description = "Number of recent changes fetched at a time.")
    int batchSize();

    @Option(defaultToNull = true, description = "If specified must be numerical indexes of Item and Property namespaces"
            + " that defined in Wikibase repository, comma separated.")
    String entityNamespaces();

    @Option(shortName = "V", longName = "verify", description = "Verify updates (may have performance impact)")
    boolean verify();

    @Option(defaultValue = "0", shortName = "T", longName = "tailPoller",
            description = "Use secondary poller with given gap (seconds) to catch up missed updates. Applies only to RecentChanges poller.")
    int tailPollerOffset();

    @Option(defaultToNull = true, shortName = "K", longName = "kafka", description = "If set, use Kafka polling with the argument as the broker server")
    String kafkaBroker();

    @Option(defaultToNull = true, shortName = "c", longName = "clusters", description = "Kafka cluster prefixes (e.g. eqiad, codfw), comma or space separated")
    List<String> clusters();

    @Option(description = "Run Updater in test mode - only report updates but do not record")
    boolean testMode();
}
