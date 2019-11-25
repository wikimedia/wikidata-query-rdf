package org.wikidata.query.rdf.tool;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.uri.UrisScheme;
import org.wikidata.query.rdf.tool.change.Change;
import org.wikidata.query.rdf.tool.change.Change.Source;
import org.wikidata.query.rdf.tool.rdf.Munger;
import org.wikidata.query.rdf.tool.rdf.RdfRepository;
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository;

import com.codahale.metrics.MetricRegistry;

/**
 * Class for testing update.
 * It does not write anything into the store but instead just logs the entities that
 * should be updated.
 *
 * @param <B> type of update batch
 */
public class TestUpdater<B extends Change.Batch> extends Updater<B> {
    private static final Logger log = LoggerFactory.getLogger(TestUpdater.class);

    private Map<String, Long> updates = new HashMap<>();

    public TestUpdater(Source<B> changeSource, WikibaseRepository wikibase,
                       RdfRepository rdfRepository, Munger munger,
                       ExecutorService executor, boolean importAsync, int pollDelay, UrisScheme uris,
                       boolean verify, MetricRegistry metricRegistry) {
        super(changeSource, wikibase, rdfRepository, munger, executor, importAsync, pollDelay, uris,
                verify, metricRegistry);
    }

    @Override
    protected void handleChanges(Collection<Change> changes, Runnable doneListener) {
        for (Change change: changes) {
            log.info("C: {} {}", change.entityId(), change);
            Long old = updates.put(change.entityId(), change.revision());
            if (old == null) {
                continue;
            }
            if (old > change.revision() && change.revision() != Change.NO_REVISION) {
                log.info("Old revision on {}: had {}, arrived {}", change.entityId(), old, change.revision());
                updates.put(change.entityId(), old);
            }
            if (old == change.revision() && old != Change.NO_REVISION) {
                log.info("Duplicate revision on {}: {}", change.entityId(), old);
            }
        }
        doneListener.run();
    }

    @Override
    protected void syncDate(Instant newDate) {
        log.info("Sync: {}", newDate);
    }

}
