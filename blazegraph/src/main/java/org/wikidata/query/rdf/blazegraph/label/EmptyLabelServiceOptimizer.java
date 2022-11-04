package org.wikidata.query.rdf.blazegraph.label;

import static org.wikidata.query.rdf.blazegraph.label.LabelServiceUtils.getLabelServiceNodes;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.IGroupNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.QueryRoot;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.SubqueryBase;
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
    private static final Logger LOG = LoggerFactory.getLogger(EmptyLabelServiceOptimizer.class);

    /**
     * Schema.org's description property as a URI.
     */
    private static final URI DESCRIPTION = new URIImpl(SchemaDotOrg.DESCRIPTION);

    private static final String LABEL_SERVICE_PROJECTION = "LabelService.projection";

    @Override
    protected void optimizeJoinGroup(AST2BOpContext ctx, StaticAnalysis sa, IBindingSet[] bSets, JoinGroupNode op) {
        final QueryRoot root = sa.getQueryRoot();
        if (root.getQueryType() == QueryType.ASK) {
            return;
        }
        if (root.getWhereClause() == op) {
            op.setProperty(LABEL_SERVICE_PROJECTION, root.getProjection());
        }
        op.getChildren(SubqueryBase.class).forEach(node -> {
            if (node.getWhereClause() != null) {
                BOp whereClause = node.getWhereClause();
                whereClause.setProperty(LABEL_SERVICE_PROJECTION, node.getProjection());
            }
        });

        // Prepare a set of vars, which might be bound both outside of the service and by LabelService
        // Fix for the issue: https://phabricator.wikimedia.org/T159723
        // See also patch for the com.bigdata.rdf.sparql.ast.eval.AST2BOpUtility.addServiceCall()
        Set<IVariable<?>> uncertainVars = collectUncertainVars(sa, bSets, op);

        getLabelServiceNodes(op).forEach(service -> {
            service.setUncertainVars(uncertainVars);
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

            if (restoreExtracted(service)) {
                foundArg = true;
            }

            if (!foundArg) {
                addResolutions(ctx, g, getProjectionNode(service));
            }

        });
    }

    private Set<IVariable<?>> collectUncertainVars(StaticAnalysis sa, IBindingSet[] bSets, JoinGroupNode op) {
        Set<IVariable<?>> uncertainVars = new HashSet<>();
        sa.getMaybeProducedBindings(op, uncertainVars, /* recursive */ true);
        for (IBindingSet bSet: bSets) {
            bSet.vars().forEachRemaining(v -> uncertainVars.add(v));
        }
        return uncertainVars;
    }

    @SuppressFBWarnings(
            value = "OCP_OVERLY_CONCRETE_PARAMETER",
            justification = "We only process ServiceNode's so that's the appropriate type")
    private ProjectionNode getProjectionNode(ServiceNode service) {
        IGroupNode<IGroupMemberNode> parent = service.getParent();
        while (parent != null) {
            ProjectionNode projection = (ProjectionNode) parent.annotations().get(
                    LABEL_SERVICE_PROJECTION
            );
            if (projection != null) {
                return projection;
            }
            parent = parent.getParent();
        }
        return null;
    }

    /**
     * Restore extracted statement from label service node.
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
            IVariable<IV> v = a.getVar();
            if (a.getValueExpression() != v) {
                continue;
            }
            /*
             * Try and match a variable name we can resolve via labels. Note
             * that we should match AltLabel before Label because Label is a
             * suffix of it....
             */
            boolean replaced = addResolutionIfSuffix(ctx, g, "AltLabel", SKOS.ALT_LABEL, v)
                    || addResolutionIfSuffix(ctx, g, "Label", RDFS.LABEL, v)
                    || addResolutionIfSuffix(ctx, g, "Description", DESCRIPTION, v);
            if (replaced && LOG.isDebugEnabled()) {
                LOG.debug("Resolving {} using a label lookup.", v);
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
            IVariable<IV> iVar) {
        if (!iVar.getName().endsWith(suffix)) {
            return false;
        }
        String source = iVar.getName().substring(0, iVar.getName().length() - suffix.length());
        IConstant<IV> labelTypeAsConstant = ctx.getAbstractTripleStore().getVocabulary().getConstant(labelType);
        g.addArg(new StatementPatternNode(new VarNode(source), new ConstantNode(labelTypeAsConstant), new VarNode(iVar)));
        return true;
    }
}
