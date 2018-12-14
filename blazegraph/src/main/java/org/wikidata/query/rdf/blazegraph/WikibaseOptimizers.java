package org.wikidata.query.rdf.blazegraph;

import org.wikidata.query.rdf.blazegraph.label.EmptyLabelServiceOptimizer;
import org.wikidata.query.rdf.blazegraph.label.LabelServiceExtractOptimizer;
import org.wikidata.query.rdf.blazegraph.label.LabelServicePlacementOptimizer;

import com.bigdata.rdf.sparql.ast.optimizers.ASTJoinGroupOrderOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.ASTQueryHintOptimizer;
import com.bigdata.rdf.sparql.ast.optimizers.DefaultOptimizerList;
import com.bigdata.rdf.sparql.ast.optimizers.IASTOptimizer;

/**
 * Optimizer list for Wikibase.
 */
public class WikibaseOptimizers extends DefaultOptimizerList {
    private static final long serialVersionUID = 2364845438265527328L;

    public WikibaseOptimizers() {
        addAfter(ASTQueryHintOptimizer.class, new LabelServicePlacementOptimizer());
        // Needs to be after wildcard projection resolution,
        // see https://phabricator.wikimedia.org/T171194
        // And before ASTJoinGroupOrderOptimizer because of BLZG-2097
        addBefore(ASTJoinGroupOrderOptimizer.class, new LabelServiceExtractOptimizer());
        add(new EmptyLabelServiceOptimizer());
    }

    /**
     * Add optimizer after optimizer of specified class.
     * @param type Optimizer class after which to insert
     * @param opt Optimizer to insert
     */
    private void addAfter(Class<?> type, final IASTOptimizer opt) {
        for (int i = 0; i < size(); i++) {
            if (type.isInstance(get(i))) {
                add(i + 1, opt);
                return;
            }
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
