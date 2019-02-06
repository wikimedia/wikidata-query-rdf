package org.wikidata.query.rdf.tool;

import static com.google.common.io.Resources.getResource;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.net.URL;

import org.wikidata.query.rdf.tool.exception.FatalException;

import com.google.common.io.Resources;

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

    /**
     * Loads some sparql related to a class.
     *
     * @param name name of the sparql file to load - the actual file loaded is
     *             %klass%.%name%.sparql.
     * @param klass Class to which this data is related - used to find the file.
     * @return contents of the sparql file
     * @throws FatalException if there is an error loading the file
     */
    public static <T> String loadBody(String name, Class<T> klass) {
        URL url = getResource(klass, klass.getSimpleName() + "." + name + ".sparql");
        try {
            return Resources.toString(url, UTF_8);
        } catch (IOException e) {
            throw new FatalException("Can't load " + url, e);
        }
    }


    private Utils() {}
}
