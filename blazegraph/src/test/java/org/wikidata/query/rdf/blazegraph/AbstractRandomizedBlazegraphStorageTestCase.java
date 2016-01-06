package org.wikidata.query.rdf.blazegraph;

import static com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope.SUITE;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.runner.RunWith;

import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.cache.SynchronizedHardReferenceQueueWithTimeout;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.TempTripleStore;
import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

/**
 * Randomized test that creates a triple store.
 *
 * <p>
 * We have to take a number of actions to make RandomizedRunner compatible with
 * Blazegraph:
 * <ul>
 * <li>Switch the ThreadLeakScope to SUITE because there are threads that
 * survive across tests
 * <li>Create a temporary store that is shared for all test methods that holds
 * multiple triple stores
 * <li>Create a new triple store per test method (lazily)
 * </ul>
 */
@RunWith(RandomizedRunner.class)
@ThreadLeakScope(SUITE)
public class AbstractRandomizedBlazegraphStorageTestCase extends RandomizedTest {

    /**
     * Holds all the triples stores. Initialized once per test class.
     */
    private static TemporaryStore temporaryStore;
    /**
     * Triple store for the current test method. Lazily initialized.
     */
    private AbstractTripleStore store;

    /**
     * Get a TemporaryStore. Lazily initialized once per test class.
     */
    private static TemporaryStore temporaryStore() {
        if (temporaryStore != null) {
            return temporaryStore;
        }
        /*
         * Initializing the temporary store replaces RandomizedRunner's
         * painstakingly applied UncaughtExceptionHandler. That is bad so we
         * replace it.
         */
        UncaughtExceptionHandler uncaughtExceptionHandler = Thread.getDefaultUncaughtExceptionHandler();
        temporaryStore = new TemporaryStore();
        Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler);
        return temporaryStore;
    }

    /**
     * Get a triple store. Lazily initialized once per test method.
     */
    protected AbstractTripleStore store() {
        if (store != null) {
            return store;
        }
        Properties properties = new Properties();
        properties.setProperty("com.bigdata.rdf.store.AbstractTripleStore.vocabularyClass",
                WikibaseVocabulary.VOCABULARY_CLASS.getName());
        properties.setProperty("com.bigdata.rdf.store.AbstractTripleStore.inlineURIFactory",
                WikibaseInlineUriFactory.class.getName());
        properties.setProperty("com.bigdata.rdf.store.AbstractTripleStore.extensionFactoryClass",
                WikibaseExtensionFactory.class.getName());
        store = new TempTripleStore(temporaryStore(), properties, null);
        return store;
    }

    /**
     * Close the temporary store used by this test.
     * @throws InterruptedException if the executor service fails to await termination
     */
    @AfterClass
    public static void closeTemporaryStore() throws InterruptedException {
        if (temporaryStore == null) {
            return;
        }
        ExecutorService executorService = temporaryStore.getExecutorService();
        temporaryStore.close();
        QueryEngine queryEngine = QueryEngineFactory.getInstance().getExistingQueryController(temporaryStore);
        if (queryEngine != null) {
            queryEngine.shutdownNow();
        }
        SynchronizedHardReferenceQueueWithTimeout.stopStaleReferenceCleaner();
        executorService.awaitTermination(20, TimeUnit.SECONDS);
        temporaryStore = null;
    }

    /**
     * Close the triple store used by the test that just finished.
     */
    @After
    public void closeStore() {
        if (store == null) {
            return;
        }
        store.close();
        store = null;
    }
}
