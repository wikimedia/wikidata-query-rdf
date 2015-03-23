package org.wikidata.query.rdf.tool.rdf;

import java.util.ArrayList;
import java.util.List;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;

import com.google.common.base.Joiner;

/**
 * Quick and dirty update builder.
 */
public class UpdateBuilder {
    private final StringBuilder prefixes = new StringBuilder();
    private final BasicPart delete = new BasicPart();
    private final BasicPart insert = new BasicPart();
    private final WherePart where = new WherePart(1, false);

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

    public WherePart whereOptional() {
        return where.optional();
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append(prefixes).append('\n');
        b.append("DELETE {\n").append(delete).append("}\n");
        b.append("INSERT {\n").append(insert).append("}\n");
        b.append("WHERE {\n").append(where).append("}\n");
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

        @Override
        public String toString() {
            return Joiner.on('\n').join(parts) + "\n";
        }

        /**
         * Properly stringify a subject, predicate, or object so it fits in the
         * update query.
         */
        private String str(Object o) {
            if (o instanceof String) {
                return (String) o;
            }
            if (o instanceof URI) {
                return "<" + o + ">";
            }
            if (o instanceof Literal) {
                return o.toString();
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

    private static class BasicPart extends AbstractPart<BasicPart> {
        private BasicPart() {
            super(1);
        }
    }

    public static class WherePart extends AbstractPart<WherePart> {
        private final boolean optional;

        private WherePart(int indent, boolean optional) {
            super(indent);
            this.optional = optional;
        }

        public WherePart optional() {
            WherePart optional = new WherePart(indent + 2, true);
            parts.add(optional);
            return optional;
        }

        @Override
        public String toString() {
            if (optional) {
                StringBuilder b = indent(new StringBuilder(), indent - 1).append("OPTIONAL {\n").append(
                        super.toString());
                indent(b, indent - 1).append("}");
                return  b.toString();
            }
            return super.toString();
        }
    }
}
