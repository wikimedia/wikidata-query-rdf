package org.wikidata.query.rdf.tool.rdf;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.util.Literals;

import com.google.common.base.Joiner;

/**
 * Quick and dirty update builder.
 */
public class UpdateBuilder {
    private final StringBuilder prefixes = new StringBuilder();
    private final BasicPart delete = new BasicPart();
    private final BasicPart insert = new BasicPart();
    private final BasicPart where = new BasicPart();

    public UpdateBuilder prefix(String prefix, String expandedForm) {
        prefixes.append("PREFIX ").append(prefix).append(": <").append(expandedForm).append(">\n");
        return this;
    }

    public UpdateBuilder delete(Object s, Object p, Object o) {
        delete.add(s, p, o);
        return this;
    }

    public UpdateBuilder insert(Object s, Object p, Object o) {
        insert.add(s, p, o);
        return this;
    }

    public UpdateBuilder where(Object s, Object p, Object o) {
        where.add(s, p, o);
        return this;
    }

    public BasicPart where() {
        return where;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append(prefixes);
        if (!delete.parts.isEmpty()) {
            b.append("DELETE {\n").append(delete).append("}\n");
        }
        if (!insert.parts.isEmpty()) {
            b.append("INSERT {\n").append(insert).append("}\n");
        }
        b.append("WHERE {\n").append(where).append("}");
        return b.toString();
    }

    private static class AbstractPart<Self extends AbstractPart<Self>> {
        protected final List<Object> parts = new ArrayList<>();
        protected final int indent;

        private AbstractPart(int indent) {
            this.indent = indent;
        }

        @SuppressWarnings("unchecked")
        public Self add(Object s, Object p, Object o) {
            parts.add(indent().append(str(s)).append(' ').append(str(p)).append(' ').append(str(o)).append(" ."));
            return (Self) this;
        }

        public NotExists notExists() {
            NotExists ne = new NotExists(indent + 1);
            parts.add(ne);
            return ne;
        }

        @Override
        public String toString() {
            return Joiner.on('\n').join(parts) + "\n";
        }

        /**
         * Properly stringify a subject, predicate, or object so it fits in the
         * update query.
         */
        protected String str(Object o) {
            if (o instanceof String) {
                // Got to escape those quotes
                return o.toString().replace("\"", "\\\"");
            }
            if (o instanceof URI) {
                return "<" + o + ">";
            }
            if (o instanceof Literal) {
                Literal l = (Literal) o;
                // This is very similar to LiteralImpl's toString but with label
                // escaping.
                StringBuilder sb = new StringBuilder(l.getLabel().length() * 2);

                sb.append('"');
                sb.append(l.getLabel().replace("\"", "\\\""));
                sb.append('"');

                if (Literals.isLanguageLiteral(l)) {
                    sb.append('@');
                    sb.append(l.getLanguage());
                } else {
                    sb.append("^^<");
                    sb.append(l.getDatatype());
                    sb.append(">");
                }

                return sb.toString();
            }
            throw new RuntimeException("I have no idea what do to with a " + o.getClass());
        }

        protected StringBuilder indent() {
            return indent(new StringBuilder(), indent);
        }

        protected StringBuilder indent(StringBuilder b, int indentation) {
            for (int i = 0; i < indentation; i++) {
                b.append("  ");
            }
            return b;
        }
    }

    public static class BasicPart extends AbstractPart<BasicPart> {
        private BasicPart() {
            super(1);
        }
    }

    public static class NotExists extends AbstractPart<NotExists> {
        private NotExists(int indent) {
            super(indent);
        }

        @Override
        public String toString() {
            StringBuilder b = indent(new StringBuilder(), indent - 1);
            b.append("FILTER NOT EXISTS {\n");
            b.append(super.toString());
            indent(b, indent - 1).append("}");
            return b.toString();
        }

        public NotExists values(Collection<Statement> statements, String... names) {
            StringBuilder b = indent().append("VALUES (");
            for (String name : names) {
                b.append(name).append(" ");
            }
            b.append(") {\n");
            for (Statement statement : statements) {
                indent(b, indent + 1).append("( ");
                b.append(str(statement.getSubject())).append(' ');
                b.append(str(statement.getPredicate())).append(' ');
                b.append(str(statement.getObject())).append(" )\n");
            }
            indent(b, indent).append("}");
            parts.add(b);
            return this;
        }
    }
}
