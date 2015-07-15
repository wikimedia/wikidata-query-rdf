package org.wikidata.query.rdf.blazegraph.label;

import org.wikidata.query.rdf.common.uri.Ontology;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.optimizers.AbstractJoinGroupOptimizer;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;

/**
 * Forces LabelService calls to the end of the query.
 */
public class LabelServicePlacementOptimizer extends AbstractJoinGroupOptimizer {
    @Override
    protected void optimizeJoinGroup(AST2BOpContext ctx, StaticAnalysis sa, IBindingSet[] bSets, JoinGroupNode op) {
        for (ServiceNode service : op.getServiceNodes()) {
            BigdataValue serviceRef = service.getServiceRef().getValue();
            if (serviceRef == null) {
                continue;
            }
            if (!serviceRef.stringValue().startsWith(Ontology.LABEL)) {
                continue;
            }
            op.removeArg(service);
            op.addArg(service);
        }
    }
}
