package org.wikidata.query.rdf.tool.options;

import com.lexicalscope.jewel.cli.Option;

import java.util.List;

/**
 * CLI options for use with JewelCli.
 */
@SuppressWarnings("checkstyle:javadocmethod")
public interface UpdateOptions extends OptionsUtils.BasicOptions, OptionsUtils.MungerOptions, OptionsUtils.WikibaseOptions {
    @Option(defaultValue = "https", description = "Wikidata url scheme")
    String wikibaseScheme();

    @Option(shortName = "s", defaultToNull = true, description = "Start time in 2015-02-11T17:11:08Z or 20150211170100 format.")
    String start();

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

    @Option(shortName = "V", longName = "verify", description = "Verify updates (may have performance impact)")
    boolean verify();

    @Option(defaultValue = "0", shortName = "T", longName = "tailPoller",
            description = "Use secondary poller with given gap (seconds) to catch up missed updates")
    int tailPollerOffset();

    @Option(defaultToNull = true, description = "If specified must be numerical indexes of Item and Property namespaces"
            + " that defined in Wikibase repository, comma separated.")
    String entityNamespaces();
}
