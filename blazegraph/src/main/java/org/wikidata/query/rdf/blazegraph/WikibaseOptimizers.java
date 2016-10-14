package org.wikidata.query.rdf.blazegraph;

import org.wikidata.query.rdf.blazegraph.label.EmptyLabelServiceOptimizer;
import org.wikidata.query.rdf.blazegraph.label.LabelServiceExtractOptimizer;
import org.wikidata.query.rdf.blazegraph.label.LabelServicePlacementOptimizer;

import com.bigdata.rdf.sparql.ast.optimizers.DefaultOptimizerList;

/**
 * Optimizer list for Wikibase.
 */
public class WikibaseOptimizers extends DefaultOptimizerList {
    private static final long serialVersionUID = 2364845438265527328L;

    public WikibaseOptimizers() {
        add(0, new LabelServiceExtractOptimizer());
        add(new EmptyLabelServiceOptimizer());
        add(new LabelServicePlacementOptimizer());
    }
}
