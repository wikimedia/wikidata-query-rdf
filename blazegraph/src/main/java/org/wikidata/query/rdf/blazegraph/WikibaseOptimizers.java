package org.wikidata.query.rdf.blazegraph;

import org.wikidata.query.rdf.blazegraph.label.EmptyLabelServiceOptimizer;
import org.wikidata.query.rdf.blazegraph.label.LabelServiceExtractOptimizer;
import org.wikidata.query.rdf.blazegraph.label.LabelServicePlacementOptimizer;
import org.wikidata.query.rdf.blazegraph.mwapi.MWApiServicePlacementOptimizer;

import com.bigdata.rdf.sparql.ast.optimizers.ASTJoinGroupOrderOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.ASTRunFirstRunLastOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.DefaultOptimizerList;
import com.bigdata.rdf.sparql.ast.optimizers.IASTOptimizer;

/**
 * Optimizer list for Wikibase.
 */
public class WikibaseOptimizers extends DefaultOptimizerList {
    private static final long serialVersionUID = 2364845438265527328L;

    public WikibaseOptimizers() {
        // There are 2 calls to LabelServicePlacementOptimizer, first one should make general rearrangement,
        // so EmptyLabelServiceOptimizer might see all possible resolutions and add corresponding statement patterns,
        // then second sweep of LabelServicePlacementOptimizer will take into account actual projected variables
        // and place service call at the lates possible position to allow assignment and other
        // projection generation nodes see the variables bound be LabelServiceCall
        addAfter(ASTRunFirstRunLastOptimizer.class, new MWApiServicePlacementOptimizer());
        addAfter(MWApiServicePlacementOptimizer.class, new LabelServicePlacementOptimizer());
        addAfter(LabelServicePlacementOptimizer.class, new EmptyLabelServiceOptimizer());
        addAfter(EmptyLabelServiceOptimizer.class, new LabelServicePlacementOptimizer());
        // Needs to be after wildcard projection resolution,
        // see https://phabricator.wikimedia.org/T171194
        // And before ASTJoinGroupOrderOptimizer because of BLZG-2097
        addBefore(ASTJoinGroupOrderOptimizer.class, new LabelServiceExtractOptimizer());
    }

    /**
     * Add optimizer after optimizer of specified class.
     * @param type Optimizer class after which to insert
     * @param opt Optimizer to insert
     */
    private void addAfter(Class<?> type, final IASTOptimizer opt) {
        // Identify the latest occurence of the optimizer with specified type
        int idx = 0;
        for (int i = 0; i < size(); i++) {
            if (type.isInstance(get(i))) {
                idx = i + 1;
            }
        }
        // insert the optimizer if its placement is identified
        if (idx > 0) {
            add(idx, opt);
            return;
        }
        throw new IllegalArgumentException("Cannot find placement for " + opt.getClass());
    }

    /**
     * Add optimizer before optimizer of specified class.
     * @param type Optimizer class before which to insert
     * @param opt Optimizer to insert
     */
    private void addBefore(Class<?> type, final IASTOptimizer opt) {
        for (int i = 0; i < size(); i++) {
            if (type.isInstance(get(i))) {
                add(i, opt);
                return;
            }
        }
        throw new IllegalArgumentException("Cannot find placement for " + opt.getClass());
    }
}
