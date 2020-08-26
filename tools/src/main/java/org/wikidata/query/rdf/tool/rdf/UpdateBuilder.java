package org.wikidata.query.rdf.tool.rdf;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;

import javax.xml.datatype.XMLGregorianCalendar;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.XMLSchema;
import org.wikidata.query.rdf.common.uri.UrisScheme;

/**
 * Quick and dirty update builder.
 */
public class UpdateBuilder {
    /**
     * The actual update text.
     */
    private String update;

    public UpdateBuilder(String template) {
        update = template;
    }

    /**
     * Bind a string to a name.
     */
    public UpdateBuilder bind(String from, String to) {
        update = update.replace('%' + from + '%', to);
        return this;
    }

    /**
     * Bind a value to a name.
     */
    public UpdateBuilder bindValue(String from, Object to) {
        update = update.replace('%' + from + '%', str(to));
        return this;
    }

    /**
     * Bind a URI to a name.
     */
    public UpdateBuilder bindUri(String from, String to) {
        bind(from, '<' + to + '>');
        return this;
    }

    /**
     * Bind a URI to a name.
     */
    public UpdateBuilder bindUri(String from, java.net.URI to) {
        bind(from, '<' + to.toString() + '>');
        return this;
    }

    /**
     * Bind some statements to a string.
     */
    public UpdateBuilder bindStatements(String from, Collection<Statement> statements) {
        StringBuilder b = new StringBuilder(statements.size() * 30);
        for (Statement s : statements) {
            b.append(str(s.getSubject())).append(' ');
            b.append(str(s.getPredicate())).append(' ');
            b.append(str(s.getObject())).append(" .\n");
        }
        bind(from, b.toString().trim());
        return this;
    }

    /**
     * Bind some values to a string.
     */
    public UpdateBuilder bindValues(String from, Collection<Statement> statements) {
        StringBuilder b = new StringBuilder(statements.size() * 30);
        for (Statement s : statements) {
            if (s.getObject() instanceof BNode || s.getSubject() instanceof BNode) {
                // skip bnodes since they can not be matched anyway
                continue;
            }
            b.append("( ").append(str(s.getSubject())).append(' ');
            b.append(str(s.getPredicate())).append(' ');
            b.append(str(s.getObject())).append(" )\n");
        }
        bind(from, b.toString().trim());
        return this;
    }

    /**
     * Bind some uris to a string.
     */
    public UpdateBuilder bindUris(String from, Collection<String> uris) {
        return bindUris(from, uris, "");
    }

    /**
     * Bind some uris to a string.
     */
    public UpdateBuilder bindUris(String from, Collection<String> uris, String prefix) {
        StringBuilder b = new StringBuilder(uris.size() * 80);

        for (String s : uris) {
            b.append('<').append(prefix).append(s).append("> ");
        }
        bind(from, b.toString().trim());
        return this;
    }

    /**
     * Bind some uris to a string.
     */
    public UpdateBuilder bindEntityIds(String from, Collection<String> ids, UrisScheme uris) {
        StringBuilder b = new StringBuilder(ids.size() * 80);

        ids.forEach(s -> b.append('<').append(uris.entityIdToURI(s)).append("> "));
        bind(from, b.toString().trim());
        return this;
    }

    @Override
    public String toString() {
        return update;
    }

    /**
     * Properly stringify a subject, predicate, or object so it fits in the
     * update query.
     */
    private String str(Object o) {
        if (o instanceof String) {
            // Got to escape those quotes
            return o.toString().replace("\"", "\\\"");
        }
        if (o instanceof URI) {
            return '<' + o.toString() + '>';
        }
        if (o instanceof XMLGregorianCalendar) {
            XMLGregorianCalendar c = (XMLGregorianCalendar) o;
            StringBuilder sb = new StringBuilder();
            sb.append('"');
            sb.append(c.toXMLFormat());
            sb.append("\"^^xsd:dateTime");
            return sb.toString();
        }
        // Instant and XML dateTime have the same string format
        if (o instanceof Instant) {
            Instant c = (Instant) o;
            StringBuilder sb = new StringBuilder();
            sb.append('"');
            sb.append(c.truncatedTo(ChronoUnit.SECONDS));
            sb.append("\"^^xsd:dateTime");
            return sb.toString();
        }
        if (o instanceof Literal) {
            Literal l = (Literal) o;
            // This is very similar to LiteralImpl's toString but with label
            // escaping.
            StringBuilder sb = new StringBuilder(l.getLabel().length() * 2);

            sb.append('"');
            sb.append(l.getLabel().replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n"));
            sb.append('"');

            if (l.getLanguage() != null) {
                sb.append('@');
                sb.append(l.getLanguage());
            } else if (!l.getDatatype().equals(XMLSchema.STRING)) {
                sb.append("^^<");
                sb.append(l.getDatatype());
                sb.append('>');
            }

            return sb.toString();
        }
        if (o instanceof BNode) {
            return "_:" + ((BNode) o).getID();
        }
        throw new RuntimeException("I have no idea what do to with a " + o.getClass());
    }
}
