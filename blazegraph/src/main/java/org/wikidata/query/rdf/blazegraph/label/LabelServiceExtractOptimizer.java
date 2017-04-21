package org.wikidata.query.rdf.blazegraph.label;

import java.util.ArrayList;
import java.util.List;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.wikidata.query.rdf.common.uri.Ontology;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.optimizers.AbstractJoinGroupOptimizer;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.store.BD;

/**
 * This class extracts label statements from label service's SERVICE clause.
 * This is to work around bug: https://jira.blazegraph.com/browse/BLZG-2097
 * EmptyLabelService will insert the statements back.
 */
public class LabelServiceExtractOptimizer extends AbstractJoinGroupOptimizer {

    /**
     * Annotation to store extracted nodes.
     */
    public static final String EXTRACTOR_ANNOTATION = LabelServiceExtractOptimizer.class.getName() + ".extractedStatements";

    @Override
    @SuppressFBWarnings(value = "EC_UNRELATED_CLASS_AND_INTERFACE", justification = "equals() is actually correct for some subtypes of BigdataValue")
    protected void optimizeJoinGroup(AST2BOpContext ctx, StaticAnalysis sa,
            IBindingSet[] bSets, JoinGroupNode op) {
        final QueryRoot root = sa.getQueryRoot();

        if (root.getQueryType() == QueryType.ASK) {
            return;
        }
        for (ServiceNode service : op.getServiceNodes()) {
            BigdataValue serviceRef = service.getServiceRef().getValue();
            if (serviceRef == null) {
                continue;
            }
            if (!serviceRef.stringValue().startsWith(Ontology.LABEL)) {
                continue;
            }
            JoinGroupNode g = (JoinGroupNode) service.getGraphPattern();
            final List<BOp> extractedNodes = new ArrayList<BOp>();
            for (BOp st : g.args()) {
                StatementPatternNode sn = (StatementPatternNode) st;
                if (sn.s().isConstant() && BD.SERVICE_PARAM.equals(sn.s().getValue())) {
                    // skip parameters
                    continue;
                }
                extractedNodes.add(st);
            }

            for (BOp node: extractedNodes) {
                g.removeArg(node);
            }

            if (!extractedNodes.isEmpty()) {
                service.annotations().put(EXTRACTOR_ANNOTATION, extractedNodes);
            }
        }
    }
}
