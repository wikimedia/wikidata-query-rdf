package org.wikidata.query.rdf.tool.rdf.client;

import java.io.IOException;

import org.eclipse.jetty.client.api.ContentResponse;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;


/**
 * A no-op response handler that skips parsing update counts from the response body.
 * Returns empty 0 regardless of the response content.
 *
 * <p>Used when metric reporting is disabled, avoiding any dependency on
 * Blazegraph-specific response formats.</p>
 */
public class DummyUpdateCountResponseHandler implements ResponseHandler<Integer> {
    @Nullable
    @Override
    public String acceptHeader() {
        return null;
    }

    @NonNull
    @Override
    public Integer parse(ContentResponse entity) throws IOException {
        return 0;
    }
}
