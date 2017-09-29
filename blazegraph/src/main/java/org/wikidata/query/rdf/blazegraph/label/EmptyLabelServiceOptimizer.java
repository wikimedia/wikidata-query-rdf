package org.wikidata.query.rdf.blazegraph.label;

import java.util.List;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.SKOS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.optimizers.AbstractJoinGroupOptimizer;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.store.BD;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Rewrites empty calls to the label service to attempt to resolve labels based
 * on the query's projection.
 */
@SuppressWarnings("rawtypes")
public class EmptyLabelServiceOptimizer extends AbstractJoinGroupOptimizer {
    private static final Logger log = LoggerFactory.getLogger(EmptyLabelServiceOptimizer.class);

    /**
     * Schema.org's description property as a URI.
     */
    private static final URI DESCRIPTION = new URIImpl(SchemaDotOrg.DESCRIPTION);

    @Override
    protected void optimizeJoinGroup(AST2BOpContext ctx, StaticAnalysis sa, IBindingSet[] bSets, JoinGroupNode op) {
        final QueryRoot root = sa.getQueryRoot();
        if (root.getQueryType() == QueryType.ASK) {
            return;
        }
        LabelServiceUtils.getLabelServiceNodes(op).forEach(service -> {
            JoinGroupNode g = (JoinGroupNode) service.getGraphPattern();
            boolean foundArg = false;
            for (BOp st : g.args()) {
                StatementPatternNode sn = (StatementPatternNode) st;
                if (sn.s().isConstant() && BD.SERVICE_PARAM.equals(sn.s().getValue())) {
                    continue;
                }
                foundArg = true;
                break;
            }

            foundArg = EmptyLabelServiceOptimizer.this.restoreExtracted(service) || foundArg;

            if (!foundArg) {
                EmptyLabelServiceOptimizer.this.addResolutions(ctx, g, root.getProjection());
            }

        });
    }

    /**
     * Restore extracted statement from label service node.
     * @param service
     * @return
     */
    @SuppressWarnings("unchecked")
    private boolean restoreExtracted(ServiceNode service) {
        boolean found = false;
        JoinGroupNode g = (JoinGroupNode) service.getGraphPattern();

        final List<StatementPatternNode> extractedList = (List<StatementPatternNode>) service
                .annotations()
                .get(LabelServiceExtractOptimizer.EXTRACTOR_ANNOTATION);
        if (extractedList != null && !extractedList.isEmpty()) {
            for (StatementPatternNode st : extractedList) {
                g.addArg(st);
            }
            found = true;
        }
        service.annotations().remove(LabelServiceExtractOptimizer.EXTRACTOR_ANNOTATION);
        return found;
    }

    /**
     * Infer that the user wanted to resolve some variables using the label
     * service.
     */
    private void addResolutions(AST2BOpContext ctx, JoinGroupNode g, ProjectionNode p) {
        if (p == null) {
            return;
        }
        for (AssignmentNode a : p) {
            IVariable<IV> var = a.getVar();
            if (a.getValueExpression() != var) {
                continue;
            }
            /*
             * Try and match a variable name we can resolve via labels. Note
             * that we should match AltLabel before Label because Label is a
             * suffix of it....
             */
            boolean replaced = addResolutionIfSuffix(ctx, g, "AltLabel", SKOS.ALT_LABEL, var)
                    || addResolutionIfSuffix(ctx, g, "Label", RDFS.LABEL, var)
                    || addResolutionIfSuffix(ctx, g, "Description", DESCRIPTION, var);
            if (replaced && log.isDebugEnabled()) {
                log.debug("Resolving {} using a label lookup.", var);
            }
        }
    }

    /**
     * Add the join group to resolve a variable if it matches a suffix,
     * returning true if it matched, false otherwise.
     */
    @SuppressFBWarnings(
            value = "OCP_OVERLY_CONCRETE_PARAMETER",
            justification = "Using AST2BOpContext makes sense since it is the only type that will ever be passed")
    private boolean addResolutionIfSuffix(AST2BOpContext ctx, JoinGroupNode g, String suffix, URI labelType,
            IVariable<IV> var) {
        if (!var.getName().endsWith(suffix)) {
            return false;
        }
        String source = var.getName().substring(0, var.getName().length() - suffix.length());
        IConstant<IV> labelTypeAsConstant = ctx.getAbstractTripleStore().getVocabulary().getConstant(labelType);
        g.addArg(new StatementPatternNode(new VarNode(source), new ConstantNode(labelTypeAsConstant), new VarNode(var)));
        return true;
    }
}
