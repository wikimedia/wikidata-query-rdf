package org.wikidata.query.rdf.updater;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class RDFDataChunk implements Serializable {
    private final String data;
    @JsonProperty("mime_type")
    private final String mimeType;
}
