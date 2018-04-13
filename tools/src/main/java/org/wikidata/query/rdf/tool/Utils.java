package org.wikidata.query.rdf.tool;

/**
 * Generic utilities.
 */
public final class Utils {
    /**
     * Find the max of two {@link Comparable}.
     */
    public static <T extends Comparable<? super T>> T max(T a, T b) {
        if (a == null) {
            return (b == null) ? null : b;
        }
        if (b == null) {
            return a;
        }
        return a.compareTo(b) >= 0 ? a : b;
    }

    private Utils() {}
}
