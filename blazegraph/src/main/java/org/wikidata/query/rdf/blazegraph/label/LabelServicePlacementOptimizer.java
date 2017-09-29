package org.wikidata.query.rdf.blazegraph.label;

import static java.lang.Boolean.TRUE;

import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.optimizers.AbstractJoinGroupOptimizer;

/**
 * Forces LabelService calls to the end of the query.
 * This will set runLast on label SERVICE if it doesn't have runLast or runFirst already.
 */
public class LabelServicePlacementOptimizer extends AbstractJoinGroupOptimizer {
    @Override
    protected void optimizeJoinGroup(AST2BOpContext ctx, StaticAnalysis sa, IBindingSet[] bSets, JoinGroupNode op) {

        LabelServiceUtils.getLabelServiceNodes(op).forEach(service -> {
            if (service.getProperty(QueryHints.RUN_LAST) != null || service.getProperty(QueryHints.RUN_FIRST) != null) {
                return;
            }
            service.setProperty(QueryHints.RUN_LAST, TRUE);
        });
    }
}
