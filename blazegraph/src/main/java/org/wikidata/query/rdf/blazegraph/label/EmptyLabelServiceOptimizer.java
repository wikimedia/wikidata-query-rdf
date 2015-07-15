package org.wikidata.query.rdf.blazegraph.label;

import org.apache.log4j.Logger;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.model.vocabulary.SKOS;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.bop.IVariable;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sparql.ast.AssignmentNode;
import com.bigdata.rdf.sparql.ast.ConstantNode;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.ProjectionNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.StaticAnalysis;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpContext;
import com.bigdata.rdf.sparql.ast.optimizers.AbstractJoinGroupOptimizer;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.store.BD;

/**
 * Rewrites empty calls to the label service to attempt to resolve labels based
 * on the query's projection.
 */
@SuppressWarnings("rawtypes")
public class EmptyLabelServiceOptimizer extends AbstractJoinGroupOptimizer {
    private static final Logger log = Logger.getLogger(EmptyLabelServiceOptimizer.class);

    /**
     * Schema.org's description property as a URI.
     */
    private static final URI DESCRIPTION = new URIImpl(SchemaDotOrg.DESCRIPTION);

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
            if (foundArg) {
                continue;
            }

            addResolutions(ctx, g, sa.getQueryRoot().getProjection());
            // We can really only do this once....
            return;
        }
    }

    /**
     * Infer that the user wanted to resolve some variables using the label
     * service.
     */
    private void addResolutions(AST2BOpContext ctx, JoinGroupNode g, ProjectionNode p) {
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
                log.debug("Resolving " + var + " using a label lookup.");
            }
        }
    }

    /**
     * Add the join group to resolve a variable if it matches a suffix,
     * returning true if it matched, false otherwise.
     */
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
