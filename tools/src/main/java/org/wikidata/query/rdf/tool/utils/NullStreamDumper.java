package org.wikidata.query.rdf.tool.utils;

import java.io.InputStream;

/**
 * StreamDumper which does not dump any content.
 */
public class NullStreamDumper implements StreamDumper {
    @Override
    public InputStream wrap(InputStream inputStream) {
        return inputStream;
    }

    @Override
    public void rotate() {
        // Null dumper does not dump and thus does not need to rotate any file.
    }

}
