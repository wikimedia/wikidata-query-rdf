package org.wikidata.query.rdf.blazegraph.ldf;

import com.bigdata.rdf.sail.webapp.BigdataRDFContext;
import com.google.gson.JsonObject;

import org.linkeddatafragments.datasource.IDataSource;
import org.linkeddatafragments.datasource.IDataSourceType;
import org.linkeddatafragments.exceptions.DataSourceCreationException;

/**
 * The type of Blazegraph-based Triple Pattern Fragment data sources.
 *
 * @author <a href="http://olafhartig.de">Olaf Hartig</a>
 */
public class BlazegraphDataSourceType implements IDataSourceType {
    /**
     * Blazegraph context.
     */
    private static BigdataRDFContext context;

    /**
     * Set Blazegraph context.
     *
     * @param rdfContext
     *            The context object.
     */
    public static void setContext(BigdataRDFContext rdfContext) {
        context = rdfContext;
    }

    @Override
    public IDataSource createDataSource(final String title,
            final String description, final JsonObject settings)
                    throws DataSourceCreationException {
        if (context == null) {
            throw new DataSourceCreationException(title, "Context is not set!");
        }

        return new BlazegraphDataSource(title, description, context);
    }

}
