package org.wikidata.query.rdf.tool.wikibase;

import lombok.Value;

@Value
public class WikibaseApiError {
    String code;
    String info;
}
