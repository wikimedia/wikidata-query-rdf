package org.wikidata.query.rdf.tool.options;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Collections.singletonList;
import static org.wikidata.query.rdf.tool.options.OptionsUtils.splitByComma;
import static org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.INPUT_DATE_FORMATTER;
import static org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.OUTPUT_DATE_FORMATTER;
import static org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.Uris.DEFAULT_ENTITY_NAMESPACES;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.http.client.utils.URIBuilder;
import org.wikidata.query.rdf.tool.exception.FatalException;
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository;

import com.lexicalscope.jewel.cli.Option;

/**
 * CLI options for use with JewelCli.
 */
@SuppressWarnings("checkstyle:javadocmethod")
public interface UpdateOptions extends OptionsUtils.BasicOptions, OptionsUtils.MungerOptions, OptionsUtils.WikibaseOptions {

    @Option(shortName = "s", defaultToNull = true, description = "Start time in 2015-02-11T17:11:08Z or 20150211170100 format.")
    String start();

    @Option(shortName = "S", defaultValue = "https", description = "Wikidata url scheme")
    String wikibaseScheme();

    @Option(shortName = "W", defaultToNull = true, description = "Wikibase instance base URL")
    String wikibaseUrl();

    @Option(shortName = "I", longName = "init", description = "Initialize last update time to start time")
    boolean init();

    @Option(shortName = "u", description = "URL to post updates and queries.")
    String sparqlUrl();

    @Option(shortName = "d", defaultValue = "10", description = "Poll delay when no updates found")
    int pollDelay();

    @Option(shortName = "t", defaultValue = "10", description = "Thread count (for wikibase entity fetch)")
    int threadCount();

    @Option(longName = "import-async", description = "Import batch asynchronously")
    boolean importAsync();

    @Option(shortName = "b", defaultValue = "100", description = "Number of recent changes fetched at a time.")
    int batchSize();

    @Option(shortName = "V", longName = "verify", description = "Verify updates (may have performance impact)")
    boolean verify();

    @Option(shortName = "T", defaultValue = "0",  longName = "tailPoller",
            description = "Use secondary poller with given gap (seconds) to catch up missed updates. Applies only to RecentChanges poller.")
    int tailPollerOffset();

    @Option(shortName = "K", defaultToNull = true, longName = "kafka", description = "If set, use Kafka polling with the argument as the broker server")
    String kafkaBroker();

    @Option(shortName = "C", defaultToNull = true, longName = "consumer", description = "Set consumer ID for Kafka poller")
    String consumerId();

    @Option(shortName = "c", defaultToNull = true, longName = "clusters", description = "Kafka cluster prefixes (e.g. eqiad, codfw), comma or space separated")
    List<String> clusters();

    @Option(defaultToNull = true, description = "If specified must be <id> or list of <id>, comma or space separated.")
    List<String> ids();

    @Option(defaultToNull = true, description = "If specified must be <start>-<end>. Ids are iterated instead of recent "
            + "changes. Start and end are inclusive.")
    String idrange();

    @Option(defaultToNull = true, description = "If specified must be numerical indexes of Item and Property namespaces"
            + " that defined in Wikibase repository, comma separated.")
    String entityNamespaces();

    @Option(description = "Load Wikibase constraints data")
    boolean constraints();

    @Option(description = "Reset Kafka offsets")
    boolean resetKafka();

    @Option(description = "Set RDF dumping in this directory", defaultToNull = true)
    String dumpDir();

    @Option(longName = "metricDomain", defaultValue = "wdqs-updater", description = "JMX metrics domain")
    String metricDomain();

    @Option(defaultValue = "0", description = "How old (hours) should revision be to start using latest revision fetch")
    int oldRevision();

    @Option(defaultValue = WikibaseRepository.Uris.DEFAULT_API_PATH, description = "Path to mediawiki api.php")
    String apiPath();

    @Option(defaultValue = WikibaseRepository.Uris.DEFAULT_ENTITY_DATA_PATH, description = "Path to Special:EntityData")
    String entityDataPath();

    static Set<Long> longEntityNamespaces(UpdateOptions updateOptions) {
        if (updateOptions.entityNamespaces() == null) return DEFAULT_ENTITY_NAMESPACES;
        return splitByComma(singletonList(updateOptions.entityNamespaces()))
                .stream()
                .map(Long::parseLong)
                .collect(toImmutableSet());
    }

    /**
     * Produce base Wikibase URL from options.
     */
    static URI getWikibaseUrl(UpdateOptions updateOptions) {
        if (updateOptions.wikibaseUrl() != null) {
            try {
                return new URI(updateOptions.wikibaseUrl());
            } catch (URISyntaxException e) {
                throw new FatalException("Unable to build Wikibase url", e);
            }
        }
        URIBuilder baseUrl = new URIBuilder();
        baseUrl.setHost(updateOptions.wikibaseHost());
        baseUrl.setScheme(updateOptions.wikibaseScheme());
        try {
            return baseUrl.build();
        } catch (URISyntaxException e) {
            throw new FatalException("Unable to build Wikibase url", e);
        }
    }

    /**
     * Create the sparql URI from the given configuration.
     *
     * @return a newly created sparql URI
     */
    static URI sparqlUri(UpdateOptions updateOptions) {
        URI sparqlUri;
        try {
            sparqlUri = new URI(updateOptions.sparqlUrl());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid url:  " + updateOptions.sparqlUrl(), e);
        }
        return sparqlUri;
    }

    @Nullable
    static Instant startInstant(UpdateOptions updateOptions) {
        if (updateOptions.start() == null) return null;
        return parseDate(updateOptions.start());
    }

    @Nullable
    static String[] parsedIds(UpdateOptions updateOptions) {
        List<String> split = splitByComma(updateOptions.ids());
        if (split == null) return null;
        return split.toArray(new String[0]);
    }

    static boolean ignoreStoredOffsets(UpdateOptions updateOptions) {
        // If we have explicit start time, we ignore kafka offsets
        return updateOptions.start() != null || updateOptions.resetKafka();
    }

    static WikibaseRepository.Uris uris(UpdateOptions updateOptions) {
        return new WikibaseRepository.Uris(
                getWikibaseUrl(updateOptions),
                UpdateOptions.longEntityNamespaces(updateOptions),
                updateOptions.apiPath(),
                updateOptions.entityDataPath());
    }

    static List<String> clusterNames(UpdateOptions updateOptions) {
        return splitByComma(updateOptions.clusters());
    }

    @Nullable
    static Path dumpDirPath(UpdateOptions updateOptions) {
        String dumpDir = updateOptions.dumpDir();
        if (dumpDir == null) return null;
        Path dumpDirPath = Paths.get(dumpDir);
        File dumpDirFile = dumpDirPath.toFile();
        if (!(dumpDirFile.exists()
                && dumpDirFile.isDirectory()
                && Files.isWritable(dumpDirPath))) {
            throw new IllegalArgumentException("Bad dump directory: " + dumpDir);
        }
        return dumpDirPath;
    }

    /**
     * Parse a string to a date, trying output format or the input format.
     *
     * @throws IllegalArgumentException if the date cannot be parsed with either format.
     */
    static Instant parseDate(String dateStr) {
        try {
            return OUTPUT_DATE_FORMATTER.parse(dateStr, Instant::from);
        } catch (DateTimeParseException e) {
            try {
                return INPUT_DATE_FORMATTER.parse(dateStr, Instant::from);
            } catch (DateTimeParseException e2) {
                throw  new IllegalArgumentException("Invalid date: " + dateStr, e2);
            }
        }
    }

    @Nullable
    static Duration revisionDuration(UpdateOptions updateOptions) {
        if (updateOptions.oldRevision() == 0) {
            return null;
        }
        return Duration.of(updateOptions.oldRevision(), ChronoUnit.HOURS);
    }
}
