package org.wikidata.query.rdf.common.uri;

/**
 * Used to prefix entities in Wikibase.
 */
public class Entity {
    /**
     * An Entity instance for wikidata.org.
     */
    public static Entity WIKIDATA = new Entity("www.wikidata.org");

    private final String namespace;
    private final Statement statement;
    private final Value value;
    private final Reference reference;
    private final Qualifier qualifier;

    public Entity(String host) {
        namespace = "http://" + host + "/entity/";
        statement = new Statement(this);
        value = new Value(this);
        reference = new Reference(this);
        qualifier = new Qualifier(this);
    }

    /**
     * Namespace of entities.
     */
    public String namespace() {
        return namespace;
    }

    /**
     * Statements prefix which wikibase uses to prefix statements.
     */
    public Statement statement() {
        return statement;
    }

    /**
     * Value prefix which wikibase uses to export values in statements.
     */
    public Value value() {
        return value;
    }

    /**
     * Reference prefix which wikibase uses to export references.
     */
    public Reference reference() {
        return reference;
    }

    /**
     * Qualifier prefix which wikibase uses to export qualifiers.
     */
    public Qualifier qualifier() {
        return qualifier;
    }

    /**
     * Adds SPARQL PREFIX declarations for all useful namespaces.
     */
    public StringBuilder prefixes(StringBuilder query) {
        query.append("PREFIX entity: <").append(namespace).append(">\n");
        query.append("PREFIX s: <").append(statement.namespace).append(">\n");
        query.append("PREFIX v: <").append(value.namespace).append(">\n");
        query.append("PREFIX r: <").append(reference.namespace).append(">\n");
        query.append("PREFIX q: <").append(qualifier.namespace).append(">\n");
        return query;
    }

    public static class Statement {
        private final String namespace;

        public Statement(Entity entity) {
            namespace = entity.namespace() + "statement/";
        }

        public String namespace() {
            return namespace;
        }
    }

    public static class Value {
        private final String namespace;

        public Value(Entity entity) {
            namespace = entity.namespace() + "value/";
        }

        public String namespace() {
            return namespace;
        }
    }

    public static class Reference {
        private final String namespace;

        public Reference(Entity entity) {
            namespace = entity.namespace() + "reference/";
        }

        public String namespace() {
            return namespace;
        }
    }

    public static class Qualifier {
        private final String namespace;

        public Qualifier(Entity entity) {
            namespace = entity.namespace() + "qualifier/";
        }

        public String namespace() {
            return namespace;
        }
    }
}
