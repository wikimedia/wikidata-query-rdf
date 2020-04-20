package org.wikidata.query.rdf.blazegraph;

import static java.lang.Boolean.TRUE;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Stream;

import org.openrdf.model.impl.URIImpl;
import org.wikidata.query.rdf.blazegraph.label.LabelService;
import org.wikidata.query.rdf.common.uri.Ontology;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sparql.ast.ArbitraryLengthPathNode;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.IBindingProducerNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.NamedSubqueryInclude;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.SubqueryBase;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.optimizers.AbstractJoinGroupOptimizer;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Finds appropriate placement for SERVICE clause.
 * The placement depends on which variables are used and projected in the SERVICE clause.
 */
public abstract class WikidataServicePlacementOptimizer extends AbstractJoinGroupOptimizer {
    /**
     * Annotations for LabelService optimizers.
     * Stores list of vars incoming (objects to be labeled) and outgoing (labels) for the service clause.
     */
    protected static final String WIKIDATA_SERVICE_IN_VARS = "WikidataService.inVars";
    protected static final String WIKIDATA_SERVICE_OUT_VARS = "WikidataService.outVars";

    /**
     * URI for service optimizer hint parameter to disable reordering. Usage: inside
     * service clause add: bd:serviceParam wikibase:disableReordering true.
     */
    public static final URIImpl DISABLE_REORDERING = new URIImpl(Ontology.NAMESPACE + "disableReordering");

    /**
     * Annotation to store optimizer hint for disabling reordering.
     */
    public static final String DISABLE_REORDERING_ANNOTATION = LabelService.class.getName() + ".disableReordering";

    @Override
    protected void optimizeJoinGroup(AST2BOpContext ctx, StaticAnalysis sa, IBindingSet[] bSets, JoinGroupNode op) {
        getServiceNodes(op, getServiceKey()).forEach(service -> {
            // Skip optimizer if service node marked with None optimizer param
            if (TRUE.equals(service.annotations().get(DISABLE_REORDERING_ANNOTATION))) {
                return;
            }

            processProjection(sa, service);

            // Rearrange ServiceNode to the latest possible position to allow for assignment and other projecting
            // nodes to see variables projected by service node
            op.removeChild(service);

            int lastJoinIndex = findLatestPossiblePositionForTheServiceNode(sa, service, op);

            op.addArg(lastJoinIndex + 1, service);

        });

    }

    private static Stream<ServiceNode> getServiceNodes(JoinGroupNode join, String prefix) {
        return join.getServiceNodes().stream().filter(node -> {
            final BigdataValue serviceRef = node.getServiceRef().getValue();
            return serviceRef != null && serviceRef.stringValue().startsWith(prefix);
        });
    }

    private int findLatestPossiblePositionForTheServiceNode(StaticAnalysis sa, BOp serviceNode,
            final JoinGroupNode joinGroup) {
        int lastJoinIndex = -1;
        // Retrieve inVars from annotations. If no WIKIDATA_SERVICE_IN_VARS annotation provided
        // (which occurs on first run of LabelServicePlacementOptimizer), we still need to
        // traverse the tree, as there might be NamedSubqueryInclude, which might
        // be producing variables for ServiceNode.
        Object inVarsObject = serviceNode.annotations().get(WIKIDATA_SERVICE_IN_VARS);
        @SuppressWarnings("unchecked")
        Set<IVariable<?>> inVars = inVarsObject instanceof Set ? (Set<IVariable<?>>) inVarsObject : Collections.emptySet();
        for (int i = joinGroup.size() - 1; i >= 0; i--) {
            BOp child = joinGroup.get(i);
            if (child != serviceNode && child instanceof IBindingProducerNode) {
                // We could not just place the service node after the last node producing bindings,
                // as it might be based on the service call projection,
                // so we are skipping nodes in bottom up direction until any node
                // will touch any inbound var for the service
                if (checkIfNodeProducesVars(sa, child, inVars)) {
                    lastJoinIndex = i;
                    break;
                }
            }
        }
        return lastJoinIndex;
    }

    /**
     * Check if this node - or its subnodes - bind any of the vars in the set.
     */
    private boolean checkIfNodeProducesVars(StaticAnalysis sa, BOp node, Set<IVariable<?>> projectedVars) {
        if (node == null) {
            return false;
        }
        if (node.args() != null) {
            for (BOp arg: node.args()) {
                if (checkIfNodeProducesVars(sa, arg, projectedVars)) {
                    return true;
                }
            }
        }
        return checkIfSpecificNodeProducesVars(sa, node, projectedVars);
    }

    private boolean checkIfSpecificNodeProducesVars(StaticAnalysis sa, BOp node, Set<IVariable<?>> projectedVars) {
        if (node instanceof Var<?>) {
            return projectedVars.contains(node);
        } else if (node instanceof AssignmentNode) {
            return checkAssignmentNode((AssignmentNode) node, projectedVars);
        } else if (node instanceof JoinGroupNode) {
            return checkJoinGroupNode(sa, (JoinGroupNode) node, projectedVars);
        } else if (node instanceof ArbitraryLengthPathNode) {
            return checkArbitraryLengthPathNode(sa, (ArbitraryLengthPathNode) node, projectedVars);
        } else if (node instanceof SubqueryBase) {
            return checkSubqueryBase(sa, (SubqueryBase) node, projectedVars);
        } else if (node instanceof NamedSubqueryInclude) {
            // We could not access NamedSubqueryInclude projection from this point, it holds only
            // string name of the named query. Also, due to bottom-up evaluation semantics, any named
            // subquery are logically evaluated first, so we need to place any service nodes afterwards.
            return true;
        } else if (node instanceof ServiceNode) {
            return checkServiceNode(sa, (ServiceNode) node, projectedVars);
        }
        return false;
    }

    private boolean checkServiceNode(StaticAnalysis sa, ServiceNode node, Set<IVariable<?>> projectedVars) {
        Set<IVariable<?>> serviceVars = sa.getMaybeProducedBindings(node);
        for (IVariable<?> serviceVar: serviceVars) {
            if (checkIfNodeProducesVars(sa, serviceVar, projectedVars)) {
                return true;
            }
        }
        return false;
    }

    private boolean checkSubqueryBase(StaticAnalysis sa, SubqueryBase node, Set<IVariable<?>> projectedVars) {
        return checkIfNodeProducesVars(sa, node.getProjection(), projectedVars);
    }

    private boolean checkArbitraryLengthPathNode(StaticAnalysis sa, ArbitraryLengthPathNode node, Set<IVariable<?>> projectedVars) {
        BOp left = node.left();
        BOp right = node.right();
        return checkIfNodeProducesVars(sa, left, projectedVars) || checkIfNodeProducesVars(sa, right, projectedVars);
    }

    @SuppressFBWarnings(
            value = "OCP_OVERLY_CONCRETE_PARAMETER",
            justification = "This function's purpose is to check JoinGroupNode instances only")
    private boolean checkJoinGroupNode(StaticAnalysis sa, JoinGroupNode node, Set<IVariable<?>> projectedVars) {
        for (BOp child: node) {
            if (checkIfNodeProducesVars(sa, child, projectedVars)) {
                return true;
            }
        }
        return false;
    }

    private boolean checkAssignmentNode(AssignmentNode node, Set<IVariable<?>> projectedVars) {
        if (projectedVars.contains(node.getVar())) {
            return true;
        }
        for (IVariable<?> v: node.getConsumedVars()) {
            if (projectedVars.contains(v)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Process the projection clause.
     */
    protected abstract void processProjection(StaticAnalysis sa, ServiceNode serviceNode);

    /**
     * The URI of the service being processed.
     */
    protected abstract String getServiceKey();
}
