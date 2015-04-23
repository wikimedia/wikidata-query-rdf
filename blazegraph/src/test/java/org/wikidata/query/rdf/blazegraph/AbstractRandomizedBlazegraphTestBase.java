package org.wikidata.query.rdf.blazegraph;

import static com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope.SUITE;

import java.lang.Thread.UncaughtExceptionHandler;
import java.math.BigInteger;
import java.util.Properties;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.runner.RunWith;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.IntegerLiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.wikidata.query.rdf.common.uri.WikibaseUris;

import com.bigdata.cache.SynchronizedHardReferenceQueueWithTimeout;
import com.bigdata.journal.TemporaryStore;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.store.ITripleStore;
import com.bigdata.rdf.store.TempTripleStore;
import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

/**
 * Randomized test that can create a triple store.
 *
 * <p>
 * We have to take a number of actions to make RandomizedRunner compatible with
 * Blazegraph:
 * <ul>
 * <li>Wwitch the ThreadLeakScope to SUITE because there are threads that
 * survive across tests
 * <li>Create a temporary store that is shared for all test methods that holds
 * multiple triple stores
 * <li>Create a triple store per test method
 * </ul>
 */
@RunWith(RandomizedRunner.class)
@ThreadLeakScope(SUITE)
public class AbstractRandomizedBlazegraphTestBase extends RandomizedTest {
    /**
     * Holds all the triples stores. Initialized once per test class.
     */
    private static TemporaryStore temporaryStore;
    /**
     * Which uris this test uses.
     */
    private WikibaseUris uris = WikibaseUris.WIKIDATA;
    /**
     * Triple store for the current test method. Lazily initialized.
     */
    private ITripleStore store;

    /**
     * The uris this test uses.
     */
    protected WikibaseUris uris() {
        return uris;
    }

    /**
     * Get a triple store. Lazily initialized once per test method.
     */
    protected ITripleStore store() {
        if (store != null) {
            return store;
        }
        Properties properties = new Properties();
        properties.setProperty("com.bigdata.rdf.store.AbstractTripleStore.vocabularyClass",
                WikibaseVocabulary.V001.class.getName());
        properties.setProperty("com.bigdata.rdf.store.AbstractTripleStore.inlineURIFactory",
                WikibaseInlineUriFactory.class.getName());
        store = new TempTripleStore(temporaryStore(), properties, null);
        return store;
    }

    /**
     * Round trip a statement through Blazegraph.
     */
    protected BigdataStatement roundTrip(Object s, Object p, Object o) {
        return roundTrip((Resource) convert(s), (URI) convert(p), convert(o));
    }

    /**
     * Round trip a statement through Blazegraph.
     */
    protected BigdataStatement roundTrip(Resource s, URI p, Value o) {
        store().addStatement(s, p, o, null);
        return store().getStatement(s, p, o, null);
    }

    /**
     * Convert any object into an RDF value.
     */
    protected Value convert(Object o) {
        if (o instanceof Value) {
            return (Value) o;
        }
        if (o instanceof String) {
            String s = (String) o;
            s = s.replaceFirst("^data:", uris.entityData());
            s = s.replaceFirst("^entity:", uris.entity());
            s = s.replaceFirst("^truthy:", uris.truthy());
            s = s.replaceFirst("^s:", uris.statement());
            s = s.replaceFirst("^v:", uris.value());
            s = s.replaceFirst("^ref:", uris.reference());
            s = s.replaceFirst("^q:", uris.qualifier());
            return new URIImpl(s);
        }
        if (o instanceof Integer) {
            return new IntegerLiteralImpl(BigInteger.valueOf((int) o));
        }
        throw new RuntimeException("No idea how to convert " + o + " to a value.  Its a " + o.getClass() + ".");
    }

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

    /**
     * Close the temporary store used by this test.
     */
    @AfterClass
    public static void closeTemporaryStore() {
        if (temporaryStore == null) {
            return;
        }
        temporaryStore.close();
        SynchronizedHardReferenceQueueWithTimeout.stopStaleReferenceCleaner();
        temporaryStore = null;
    }
}
