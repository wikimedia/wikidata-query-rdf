package org.wikidata.query.rdf.tool.change;

import static java.time.temporal.ChronoUnit.DAYS;

import java.net.URI;
import java.time.Instant;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.tool.options.UpdateOptions;
import org.wikidata.query.rdf.tool.rdf.RdfRepository;
import org.wikidata.query.rdf.tool.rdf.client.RdfClient;
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository;

/**
 * Provides methods to initialize change sources.
 * Depends on options provided on the command line for the choice of source.
 * @see UpdateOptions
 */
public final class ChangeSourceContext {
    private static final Logger log = LoggerFactory.getLogger(ChangeSourceContext.class);

    private static final String MAX_DAYS_BACK_NAME = "wikibaseMaxDaysBack";

    private ChangeSourceContext() {
        // Utility class should never be constructed
    }

    /**
     * Build a change source.
     *
     * @return the change source
     * @throws IllegalArgumentException if the options are invalid
     * @throws IllegalStateException if the left off date is too ancient
     */
    @Nonnull
    public static Change.Source<? extends Change.Batch> buildChangeSource(
            UpdateOptions options, Instant startTime,
            WikibaseRepository wikibaseRepository, RdfClient rdfClient, URI root) {
        if (options.idrange() != null) {
            return buildIdRangeChangeSource(options.idrange(), options.batchSize());
        }
        if (options.ids() != null) {
            return new IdListChangeSource(UpdateOptions.parsedIds(options), options.batchSize());
        }

        if (options.kafkaBroker() != null) {
            KafkaOffsetsRepository kafkaOffsetsRepository = new KafkaOffsetsRepository(root, rdfClient);
            return KafkaPoller.buildKafkaPoller(
                    options.kafkaBroker(),
                    options.consumerId(),
                    UpdateOptions.clusterNames(options),
                    wikibaseRepository.getUris(),
                    options.batchSize(),
                    startTime,
                    UpdateOptions.ignoreStoredOffsets(options),
                    kafkaOffsetsRepository);
        }
        return new RecentChangesPoller(
                wikibaseRepository,
                startTime,
                options.batchSize(),
                options.tailPollerOffset()
        );
    }

    /**
     * Determine instant from which to start polling.
     * @param startInstant Instant passed from command-line
     * @param rdfRepository RDF repository that may contain the timestamp we left off last time
     * @param init Should we record the initial instant to the repository?
     * @return Instant with which to start polling
     */
    @Nonnull
    public static Instant getStartTime(Instant startInstant, RdfRepository rdfRepository, boolean init) {
        if (startInstant != null) {
            if (init) {
                // Initialize left off time to start time
                rdfRepository.updateLeftOffTime(startInstant);
            }
        } else {
            log.info("Checking where we left off");
            Instant leftOff = rdfRepository.fetchLeftOffTime();
            Integer maxDays = Integer.valueOf(System.getProperty(MAX_DAYS_BACK_NAME, "30"));
            Instant minStartTime = Instant.now().minus(maxDays.intValue(), DAYS);
            if (leftOff == null) {
                startInstant = minStartTime;
                log.info("Defaulting start time to {} days ago: {}", maxDays, startInstant);
            } else {
                if (leftOff.isBefore(minStartTime)) {
                    throw new IllegalStateException("RDF store reports the last update time is before the minimum safe poll time.  "
                            + "You will have to reload from scratch or you might have missing data.");
                }
                startInstant = leftOff;
                log.info("Found start time in the RDF store: {}", leftOff);
            }
        }
        return startInstant;
    }

    /**
     * Builds a change source based on a range of IDs.
     */
    private static Change.Source<? extends Change.Batch> buildIdRangeChangeSource(String idrange, int batchSize) {
        String[] ids = idrange.split("-");
        long start;
        long end;
        switch (ids.length) {
        case 1:
            // FIXME: this seems to be an undocumented abuse of the --idrange option, should we drop support?
            // Dropping support would make it easier to move this parsing to the UpdateOptions class.
            if (!Character.isDigit(ids[0].charAt(0))) {
                // Not a digit - probably just single ID
                return new IdListChangeSource(ids, batchSize);
            }
            start = Long.parseLong(ids[0]);
            end = start;
            break;
        case 2:
            start = Long.parseLong(ids[0]);
            end = Long.parseLong(ids[1]);
            break;
        default:
            throw new IllegalArgumentException("Invalid format for --idrange.  Need <start>-<stop>.");
        }
        return IdRangeChangeSource.forItems(start, end, batchSize);
    }
}
