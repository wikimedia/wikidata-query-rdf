package org.wikidata.query.rdf.tool.rdf.client;

import java.io.IOException;

import org.eclipse.jetty.client.api.ContentResponse;
import org.wikidata.query.rdf.tool.rdf.CollectedUpdateMetrics;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;


/**
 * A no-op response handler that skips parsing update metrics from the response body.
 * Returns empty {@link CollectedUpdateMetrics} regardless of the response content.
 *
 * <p>Used when metric reporting is disabled, avoiding any dependency on
 * Blazegraph-specific response formats.</p>
 */
public class DummyUpdateMetricsResponseHandler implements ResponseHandler<CollectedUpdateMetrics> {

    @Nullable
    @Override
    public String acceptHeader() {
        return null;
    }


    /**
     * Returns empty metrics without parsing the response.
     *
     * @param entity
     * @return
     * @throws IOException
     */
    @NonNull
    @Override
    public CollectedUpdateMetrics parse(ContentResponse entity) throws IOException {
        return new CollectedUpdateMetrics();
    }
}
