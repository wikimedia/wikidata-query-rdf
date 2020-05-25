package org.wikidata.query.rdf.tool.stream;

import org.wikidata.query.rdf.tool.rdf.RDFPatch;

import lombok.Value;

public interface StreamConsumer {
    Batch poll(long timeout);

    void acknowledge();

    void close();

    @Value
    class Batch {
        RDFPatch patch;
    }
}
