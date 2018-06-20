package org.wikidata.query.rdf.tool.rdf.client;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.eclipse.jetty.client.api.ContentResponse;
import org.openrdf.query.QueryResultHandlerException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.TupleQueryResultBuilder;
import org.openrdf.query.resultio.QueryResultParseException;
import org.openrdf.query.resultio.binary.BinaryQueryResultParser;

/**
 * Parses responses to regular queries into TupleQueryResults.
 */
class TupleQueryResponse implements ResponseHandler<TupleQueryResult> {
    @Override
    public String acceptHeader() {
        return "application/x-binary-rdf-results-table";
    }

    @Override
    public TupleQueryResult parse(ContentResponse entity) throws IOException {
        BinaryQueryResultParser p = new BinaryQueryResultParser();
        TupleQueryResultBuilder collector = new TupleQueryResultBuilder();
        p.setQueryResultHandler(collector);
        try {
            p.parseQueryResult(new ByteArrayInputStream(entity.getContent()));
        } catch (QueryResultParseException | QueryResultHandlerException | IllegalStateException e) {
            throw new RuntimeException("Error parsing query", e);
        }
        return collector.getQueryResult();
    }
}
