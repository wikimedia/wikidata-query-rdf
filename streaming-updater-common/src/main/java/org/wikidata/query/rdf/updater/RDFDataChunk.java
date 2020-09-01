package org.wikidata.query.rdf.updater;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class RDFDataChunk {
    private final String data;
    @JsonProperty("mime_type")
    private final String mimeType;
}
