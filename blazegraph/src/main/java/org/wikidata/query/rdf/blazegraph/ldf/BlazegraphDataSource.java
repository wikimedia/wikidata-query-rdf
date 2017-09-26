package org.wikidata.query.rdf.blazegraph.ldf;

import org.linkeddatafragments.datasource.DataSourceBase;
import org.linkeddatafragments.datasource.IFragmentRequestProcessor;
import org.linkeddatafragments.fragments.IFragmentRequestParser;

import com.bigdata.rdf.sail.webapp.BigdataRDFContext;
import com.bigdata.rdf.store.AbstractTripleStore;

/**
 * A Blazegraph-based data source.
 *
 * @author <a href="http://olafhartig.de">Olaf Hartig</a>
 */
public class BlazegraphDataSource extends DataSourceBase {
    /**
     * Blazegraph context.
     */
    private final BigdataRDFContext context;

    public BlazegraphDataSource(final String title, final String description,
            final BigdataRDFContext context) {
        super(title, description);
        this.context = context;
    }

    /**
     * Get store for current request.
     *
     * @return Triple store.
     */
    protected AbstractTripleStore getStore() {
        return context.getTripleStore(context.getConfig().namespace, -1);
    }

    @Override
    public IFragmentRequestParser getRequestParser() {
        return new TPFRequestParserForBlazegraph(
                getStore().getLexiconRelation());
    }

    @Override
    public IFragmentRequestProcessor getRequestProcessor() {
        return new BlazegraphBasedTPFRequestProcessor(getStore());
    }

}
