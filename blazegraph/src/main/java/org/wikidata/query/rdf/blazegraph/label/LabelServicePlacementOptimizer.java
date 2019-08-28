package org.wikidata.query.rdf.blazegraph.label;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.wikidata.query.rdf.blazegraph.WikidataServicePlacementOptimizer;
import org.wikidata.query.rdf.blazegraph.label.LabelService.Resolution;

import com.bigdata.bop.IVariable;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;

/**
 * Finds appropriate placement for SERVICE clause.
 * The placement depends on which variables are used and projected in the SERVICE clause.
 */
public class LabelServicePlacementOptimizer extends WikidataServicePlacementOptimizer {

    @Override
    protected void processProjection(StaticAnalysis sa, ServiceNode serviceNode) {
        if (serviceNode.getProjectedVars() == null) {
            List<Resolution> resolutions = LabelService.findResolutions(serviceNode);
            if (resolutions.size() > 0) {
                // If resolutions could be resolved, assume them as projected vars for the service node
                // this serves several purposes: we need these vars to check against variables used in
                // AssignmentNode(s) to properly place ServiceNode before these assignment nodes
                // and secondly, these projected vars become visible to subsequent optimizers so they
                // might use this information to apply further reordering properly.
                Set<IVariable<?>> inVars = new HashSet<>();
                Set<IVariable<?>> outVars = new HashSet<>();
                for (Resolution resolution: resolutions) {
                    if (resolution.subject() instanceof IVariable) {
                        inVars.add((IVariable<?>)resolution.subject());
                    }
                    outVars.add(resolution.target());
                }
                serviceNode.annotations().put(WIKIDATA_SERVICE_IN_VARS, inVars);
                serviceNode.annotations().put(WIKIDATA_SERVICE_OUT_VARS, outVars);
                HashSet<IVariable<?>> projectedVars = new HashSet<>(inVars);
                projectedVars.addAll(outVars);
                serviceNode.setProjectedVars(projectedVars);
            }
        }
    }

    @Override
    protected String getServiceKey() {
        return LabelService.SERVICE_KEY.stringValue();
    }
}
