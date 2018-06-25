package org.wikidata.query.rdf.tool.rdf.client;

import java.io.IOException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.eclipse.jetty.client.api.ContentResponse;

/**
 * Passed to execute to setup the accept header and parse the response. Its
 * super ultra mega important to parse the response in execute because
 * execute manages closing the http response object. If execute return the
 * input stream after closing the response then everything would
 * <strong>mostly</strong> work but things would blow up with strange socket
 * closed errors.
 *
 * @param <T> the type of response parsed
 */
interface ResponseHandler<T> {
    /**
     * The contents of the accept header sent to the rdf repository.
     */
    @Nullable
    String acceptHeader();

    /**
     * Parse the response.
     *
     * @throws IOException if there is an error reading the response
     */
    @Nonnull
    T parse(ContentResponse entity) throws IOException;
}
