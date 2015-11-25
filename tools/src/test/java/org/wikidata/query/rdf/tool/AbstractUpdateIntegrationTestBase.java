package org.wikidata.query.rdf.tool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.wikidata.query.rdf.common.uri.WikibaseUris;
import org.wikidata.query.rdf.tool.change.Change;
import org.wikidata.query.rdf.tool.change.IdRangeChangeSource;
import org.wikidata.query.rdf.tool.rdf.Munger;
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository;

/**
 * Superclass for tests that need to run a full update.
 */
public class AbstractUpdateIntegrationTestBase extends AbstractRdfRepositoryIntegrationTestBase {
    /**
     * Wikibase test against.
     */
    private final WikibaseRepository wikibaseRepository = new WikibaseRepository("https", "www.wikidata.org");
    /**
     * Munger to test against.
     */
    private final Munger munger = new Munger(WikibaseUris.WIKIDATA).removeSiteLinks();

    /**
     * Update all ids from from to to.
     */
    public void update(int from, int to) {
        Change.Source<?> source = IdRangeChangeSource.forItems(from, to, 30);
        ExecutorService executorService = new ThreadPoolExecutor(0, 10, 0, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>());
        WikibaseUris uris = new WikibaseUris("www.wikidata.org");
        Update<?> update = new Update<>(source, wikibaseRepository, rdfRepository(), munger, executorService, 0, uris, false);
        update.run();
        executorService.shutdown();
    }

    /**
     * Update the specified id.
     */
    public void update(int id) {
        update(id, id);
    }
}
