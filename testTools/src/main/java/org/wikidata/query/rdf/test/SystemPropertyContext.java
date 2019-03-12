package org.wikidata.query.rdf.test;

import java.io.Closeable;

/**
 * Used to set system properties in a specific test context and reset them afterward.
 */
public final class SystemPropertyContext implements Closeable {

    private final String key;
    private final String value;

    private SystemPropertyContext(String key, String value) {
        this.key = key;
        this.value = value;
    }

    /**
     * Set a system property.
     *
     * The returned Closable should be used in a try-with-resources block to ensure state is cleaned up.
     */
    public static Closeable setProperty(String key, String value) {
        String currentValue = System.getProperty(key);
        System.setProperty(key, value);
        return new SystemPropertyContext(key, currentValue);
    }


    /**
     * Reset a system property to its previous state.
     */
    @Override
    public void close() {
        if (value == null) System.clearProperty(key);
        else System.setProperty(key, value);
    }
}
