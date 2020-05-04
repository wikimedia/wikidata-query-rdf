package org.wikidata.query.rdf.tool.rdf;

import java.util.function.UnaryOperator;

import org.openrdf.model.BNode;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;

public class BNodeSkolemization implements UnaryOperator<Statement> {
    private final ValueFactory valueFactory;
    private final String skolemIRIPrefix;

    public BNodeSkolemization(ValueFactory valueFactory, String skolemIRIPrefix) {
        this.valueFactory = valueFactory;
        this.skolemIRIPrefix = skolemIRIPrefix;
    }

    @Override
    public Statement apply(Statement statement) {
        Value object = statement.getObject();
        Resource subject = statement.getSubject();
        boolean changed = false;
        if (subject instanceof BNode) {
            subject = skolemize((BNode) statement.getSubject());
            changed = true;
        }
        if (object instanceof BNode) {
            object = skolemize((BNode) statement.getObject());
            changed = true;
        }
        if (!changed) {
            return statement;
        }
        if (statement.getContext() != null) {
            return valueFactory.createStatement(subject, statement.getPredicate(), object, statement.getContext());
        } else {
            return valueFactory.createStatement(subject, statement.getPredicate(), object);
        }
    }

    private URI skolemize(BNode node) {
        return valueFactory.createURI(skolemIRIPrefix + node.getID());
    }
}

