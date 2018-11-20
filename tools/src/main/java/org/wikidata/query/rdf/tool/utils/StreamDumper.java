package org.wikidata.query.rdf.tool.utils;

import java.io.InputStream;

/**
 * Wraps OutputStreams and dump the content that has been read.
 */
public interface StreamDumper {
    /**
     * Wraps a stream and dumps its content once it is closed.
     */
    InputStream wrap(InputStream inputStream);

    /**
     * Call this method when the output should be rotated.
     */
    void rotate();
}
