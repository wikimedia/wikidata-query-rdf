package org.wikidata.query.rdf.blazegraph.label;

import static org.wikidata.query.rdf.blazegraph.label.LabelService.DISABLE_REORDERING_ANNOTATION;
import static org.wikidata.query.rdf.blazegraph.label.LabelService.LABEL_SERVICE_IN_VARS;
import static org.wikidata.query.rdf.blazegraph.label.LabelService.LABEL_SERVICE_OUT_VARS;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.wikidata.query.rdf.blazegraph.label.LabelService.Resolution;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
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
public class LabelServicePlacementOptimizer extends AbstractJoinGroupOptimizer {
    @Override
    protected void optimizeJoinGroup(AST2BOpContext ctx, StaticAnalysis sa, IBindingSet[] bSets, JoinGroupNode op) {
        LabelServiceUtils.getLabelServiceNodes(op).forEach(service -> {
            // Skip optimizer if service node marked with None optimizer param
            if (Boolean.TRUE.equals(service.annotations().get(DISABLE_REORDERING_ANNOTATION))) {
                return;
            }

            processProjection(service);

            // Rearrange ServiceNode to the latest possible position to allow for assignment and other projecting
            // nodes to see variables projected by service node
            op.removeChild(service);

            int lastJoinIndex = findLatestPossiblePositionForTheLabelServiceNode(sa, service, op);

            op.addArg(lastJoinIndex + 1, service);

        });

    }

    private int findLatestPossiblePositionForTheLabelServiceNode(StaticAnalysis sa, BOp serviceNode,
            final JoinGroupNode joinGroup) {
        int lastJoinIndex = -1;
        // Retrieve inVars from annotations. If no LABEL_SERVICE_IN_VARS annotation provided
        // (which occurs on first run of LabelServicePlacementOptimizer), we still need to
        // traverse the tree, as there might be NamedSubqueryInclude, which might
        // be producing variables for ServiceNode.
        Object inVarsObject = serviceNode.annotations().get(LABEL_SERVICE_IN_VARS);
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
        for (IVariable<?> var: node.getConsumedVars()) {
            if (projectedVars.contains(var)) {
                return true;
            }
        }
        return false;
    }

    private void processProjection(ServiceNode serviceNode) {
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
                serviceNode.annotations().put(LABEL_SERVICE_IN_VARS, inVars);
                serviceNode.annotations().put(LABEL_SERVICE_OUT_VARS, outVars);
                HashSet<IVariable<?>> projectedVars = new HashSet<>(inVars);
                projectedVars.addAll(outVars);
                serviceNode.setProjectedVars(projectedVars);
            }
        }
    }
}
