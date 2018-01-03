package org.wikidata.query.rdf.tool;

import static org.wikidata.query.rdf.test.CloseableRule.autoClose;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.runner.RunWith;
import org.wikidata.query.rdf.common.uri.WikibaseUris;
import org.wikidata.query.rdf.test.CloseableRule;
import org.wikidata.query.rdf.tool.change.Change;
import org.wikidata.query.rdf.tool.change.IdRangeChangeSource;
import org.wikidata.query.rdf.tool.rdf.Munger;
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository;

import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.RandomizedTest;

/**
 * Superclass for tests that need to run a full update.
 */
@RunWith(RandomizedRunner.class)
public class AbstractUpdaterIntegrationTestBase extends RandomizedTest {
    /**
     * Wikibase test against.
     */
    @Rule
    public final CloseableRule<WikibaseRepository> wikibaseRepository = autoClose(new WikibaseRepository("https", "www.wikidata.org"));
    /**
     * Munger to test against.
     */
    private final Munger munger = new Munger(WikibaseUris.getURISystem()).removeSiteLinks();

    /**
     * Repository to test with.
     */
    @Rule
    public RdfRepositoryForTesting rdfRepository = new RdfRepositoryForTesting("wdq");


    /**
     * Update all ids from from to to.
     */
    public void update(int from, int to) {
        Change.Source<?> source = IdRangeChangeSource.forItems(from, to, 30);
        ExecutorService executorService = new ThreadPoolExecutor(0, 10, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        WikibaseUris uris = new WikibaseUris("www.wikidata.org");
        try (Updater<?> updater = new Updater<>(source, wikibaseRepository.get(), rdfRepository, munger, executorService, 0, uris, false)) {
            updater.run();
        }
    }

    /**
     * Update the specified id.
     */
    public void update(int id) {
        update(id, id);
    }
}
