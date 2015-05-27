package org.wikidata.query.rdf.tool;

import java.util.Collection;

import org.openrdf.model.Statement;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;

/**
 * Enriches collections of {@link Statement}s with handy utility methods for
 * filtering based on common criteria.
 */
public class FilteredStatements {

    /**
     * The collection of {@link Statement}s to enrich.
     */
    private final Collection<Statement> statements;

    /**
     * Enriches the given collection of {@link Statement}s.
     */
    public static FilteredStatements filtered(Collection<Statement> statements) {
        return new FilteredStatements(statements);
    }

    /**
     * Enriches the given collection of {@link Statement}s.
     */
    public FilteredStatements(Collection<Statement> statements) {
        this.statements = statements;
    }

    /**
     * Returns a collection of statements, filtering out any with a subject
     * different from the given {@link String}.
     */
    public Collection<Statement> withSubject(final String subject) {
        Predicate<Statement> aboutSubject = new Predicate<Statement>() {
            @Override
            public boolean apply(Statement statement) {
                return subject.equals(statement.getSubject().stringValue());
            }
        };
        return Collections2.filter(statements, aboutSubject);
    }

    /**
     * Returns a collection of statements that have subject start with given {@link String}.
     */
    public Collection<Statement> withSubjectStarts(final String prefix) {
        Predicate<Statement> aboutSubject = new Predicate<Statement>() {
            @Override
            public boolean apply(Statement statement) {
                return statement.getSubject().stringValue().startsWith(prefix);
            }
        };
        return Collections2.filter(statements, aboutSubject);
    }
}
