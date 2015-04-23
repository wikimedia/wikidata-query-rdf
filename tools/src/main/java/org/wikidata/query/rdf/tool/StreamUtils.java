package org.wikidata.query.rdf.tool;

import static com.google.common.base.Charsets.UTF_8;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;

/**
 * Utilities for working with streams.
 */
public final class StreamUtils {
    /**
     * Read an input stream as UTF-8 characters.
     */
    public static Reader utf8(InputStream stream) {
        return new InputStreamReader(stream, UTF_8);
    }

    /**
     * Wrap an output stream in a writer that writes UTF_8 characters.
     */
    public static Writer utf8(OutputStream stream) {
        return new OutputStreamWriter(stream, UTF_8);
    }

    private StreamUtils() {
        // Uncallable util constructor
    }
}
