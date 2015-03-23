package org.wikidata.query.rdf.tool;

import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.wikidata.query.rdf.common.uri.Entity;

/**
 * Constructs statements for testing.
 */
public class StatementHelper {
    /**
     * Statement constructor taking just URIs as strings.
     */
    public static Statement statement(String s, String p, String o) {
        return statement(s, p, uri(o));
    }

    /**
     * Statement constructor with a value. Use this one for string values.
     */
    public static Statement statement(String s, String p, Value o) {
        return new StatementImpl(uri(s), uri(p), o);
    }

    /**
     * Convert a string into a URI for testing.
     */
    public static URI uri(String r) {
        if (r.startsWith("Q") || r.startsWith("P")) {
            return new URIImpl(Entity.WIKIDATA.namespace() + r);
        }
        return new URIImpl(r);
    }
}
