package org.wikidata.query.rdf.updater;

public interface DiffEventData extends MutationEventData {
    RDFDataChunk getRdfAddedData();
    RDFDataChunk getRdfDeletedData();
    RDFDataChunk getRdfLinkedSharedData();
    RDFDataChunk getRdfUnlinkedSharedData();
}
