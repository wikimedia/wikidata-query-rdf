package org.wikidata.query.rdf.tool.rdf;

import java.util.Collection;

import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.XMLSchema;

/**
 * Quick and dirty update builder.
 */
public class UpdateBuilder {
    private String template;

    public UpdateBuilder(String template) {
        this.template = template;
    }

    public UpdateBuilder bind(String from, String to) {
        template = template.replace('%' + from + '%', to);
        return this;
    }

    public UpdateBuilder bindUri(String from, String to) {
        bind(from, '<' + to + '>');
        return this;
    }

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

    public UpdateBuilder bindValues(String from, Collection<Statement> statements) {
        StringBuilder b = new StringBuilder(statements.size() * 30);
        for (Statement s : statements) {
            b.append("( ").append(str(s.getSubject())).append(' ');
            b.append(str(s.getPredicate())).append(' ');
            b.append(str(s.getObject())).append(" )\n");
        }
        bind(from, b.toString().trim());
        return this;
    }

    @Override
    public String toString() {
        return template;
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
        if (o instanceof Literal) {
            Literal l = (Literal) o;
            // This is very similar to LiteralImpl's toString but with label
            // escaping.
            StringBuilder sb = new StringBuilder(l.getLabel().length() * 2);

            sb.append('"');
            sb.append(l.getLabel().replace("\\", "\\\\").replace("\"", "\\\""));
            sb.append('"');

            if (l.getLanguage() != null) {
                sb.append('@');
                sb.append(l.getLanguage());
            } else if (!l.getDatatype().equals(XMLSchema.STRING)) {
                sb.append("^^<");
                sb.append(l.getDatatype());
                sb.append(">");
            }

            return sb.toString();
        }
        throw new RuntimeException("I have no idea what do to with a " + o.getClass());
    }
}
