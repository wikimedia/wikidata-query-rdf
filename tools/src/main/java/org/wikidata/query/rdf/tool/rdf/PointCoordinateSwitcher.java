package org.wikidata.query.rdf.tool.rdf;

import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.wikidata.query.rdf.common.WikibasePoint;
import org.wikidata.query.rdf.common.uri.GeoSparql;
import org.wikidata.query.rdf.tool.rdf.Munger.FormatHandler;

/**
 * Statement handler that switches coordinate order for all points.
 */
public class PointCoordinateSwitcher implements FormatHandler {

    @Override
    public Statement handle(Statement statement) {
        Value object = statement.getObject();
        if (object instanceof Literal) {
            Literal lit = (Literal)object;
            if (lit.getDatatype().toString().equals(GeoSparql.WKT_LITERAL)) {
                WikibasePoint point = new WikibasePoint(lit.getLabel());
                // Produce statement with coordinate order switched
                return new StatementImpl(statement.getSubject(),
                        statement.getPredicate(),
                        new LiteralImpl(point.toOrder(WikibasePoint.DEFAULT_ORDER.getOther()),
                                        new URIImpl(GeoSparql.WKT_LITERAL)));
            }
        }
        return statement;
    }

}
