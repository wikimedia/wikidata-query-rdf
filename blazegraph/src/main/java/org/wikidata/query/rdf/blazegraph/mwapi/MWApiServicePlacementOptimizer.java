package org.wikidata.query.rdf.blazegraph.mwapi;

import org.wikidata.query.rdf.blazegraph.WikidataServicePlacementOptimizer;

import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;

/**
 * Optimizer for MWAPI service block, to find the right placement for it.
 */
public class MWApiServicePlacementOptimizer extends WikidataServicePlacementOptimizer {

    @Override
    protected void processProjection(StaticAnalysis sa, ServiceNode serviceNode) {
        serviceNode.annotations().put(WIKIDATA_SERVICE_IN_VARS, serviceNode.getRequiredBound(sa));
    }

    @Override
    protected String getServiceKey() {
        return MWApiServiceFactory.SERVICE_KEY.stringValue();
    }

}
