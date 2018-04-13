package org.wikidata.query.rdf.blazegraph.label;

import java.util.stream.Stream;

import org.wikidata.query.rdf.common.uri.Ontology;

import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;

/**
 * Utilities for label service.
 */
public final class LabelServiceUtils {

    /**
     * Empty ctor.
     */
    private LabelServiceUtils() {}

    /**
     * Get label service nodes in given join group.
d     */
    public static Stream<ServiceNode> getLabelServiceNodes(JoinGroupNode join) {
        return join.getServiceNodes().stream().filter(node -> {
            final BigdataValue serviceRef = node.getServiceRef().getValue();
            return serviceRef != null && serviceRef.stringValue().startsWith(Ontology.LABEL);
        });
    }
}
