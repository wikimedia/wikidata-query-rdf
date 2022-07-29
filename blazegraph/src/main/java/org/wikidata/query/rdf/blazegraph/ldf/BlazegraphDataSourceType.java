package org.wikidata.query.rdf.blazegraph.ldf;

import org.linkeddatafragments.datasource.IDataSource;
import org.linkeddatafragments.datasource.IDataSourceType;
import org.linkeddatafragments.exceptions.DataSourceCreationException;

import com.bigdata.rdf.sail.webapp.BigdataRDFContext;
import com.google.gson.JsonObject;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

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
    @SuppressFBWarnings(value = "EI_EXPOSE_STATIC_REP2", justification = "historical, would require a significant refactoring to fix")
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
