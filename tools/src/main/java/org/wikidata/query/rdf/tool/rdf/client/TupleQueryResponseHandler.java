package org.wikidata.query.rdf.tool.rdf.client;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.function.Supplier;

import org.eclipse.jetty.client.api.ContentResponse;
import org.openrdf.query.QueryResultHandlerException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.impl.TupleQueryResultBuilder;
import org.openrdf.query.resultio.QueryResultParseException;
import org.openrdf.query.resultio.TupleQueryResultParser;
import org.openrdf.query.resultio.binary.BinaryQueryResultParser;
import org.openrdf.query.resultio.sparqljson.SPARQLResultsJSONParser;

import com.fasterxml.jackson.core.JsonParseException;

/**
 * Parses responses to regular queries into TupleQueryResults.
 */
class TupleQueryResponseHandler implements ResponseHandler<TupleQueryResult> {

    private final ResponseFormat format;

    TupleQueryResponseHandler(ResponseFormat format) {
        this.format = format;
    }
    @Override
    public String acceptHeader() {
        return format.mime;
    }

    @Override
    public TupleQueryResult parse(ContentResponse entity) throws IOException {
        TupleQueryResultParser p = format.parserSupplier.get();
        TupleQueryResultBuilder collector = new TupleQueryResultBuilder();
        p.setQueryResultHandler(collector);
        try {
            p.parseQueryResult(new ByteArrayInputStream(entity.getContent()));
        } catch (QueryResultParseException | QueryResultHandlerException | IllegalStateException | JsonParseException e) {
            throw new RuntimeException("Error parsing query", e);
        }
        return collector.getQueryResult();
    }

    public enum ResponseFormat {
        BINARY("application/x-binary-rdf-results-table", BinaryQueryResultParser::new),
        JSON("application/json", SPARQLResultsJSONParser::new);
        private final String mime;
        private final Supplier<TupleQueryResultParser> parserSupplier;

        ResponseFormat(String mime, Supplier<TupleQueryResultParser> parserSupplier) {
            this.mime = mime;
            this.parserSupplier = parserSupplier;
        }
    }
}
