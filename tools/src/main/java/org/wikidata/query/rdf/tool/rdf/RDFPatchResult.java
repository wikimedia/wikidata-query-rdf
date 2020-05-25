package org.wikidata.query.rdf.tool.rdf;

import lombok.Value;

@Value
public class RDFPatchResult {
    int expectedMutations;
    int actualMutations;
    int possibleSharedElementMutations;
    int actualSharedElementsMutations;
}
