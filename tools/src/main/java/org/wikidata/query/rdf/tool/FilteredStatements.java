package org.wikidata.query.rdf.tool;

import static com.google.common.collect.ImmutableList.toImmutableList;

import java.util.Collection;

import org.openrdf.model.Statement;

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
        return statements.stream()
                .filter(statement -> subject.equals(statement.getSubject().stringValue()))
                .collect(toImmutableList());
    }

    /**
     * Returns a collection of statements that have subject start with given {@link String}.
     */
    public Collection<Statement> withSubjectStarts(final String prefix) {
        return statements.stream()
                .filter(statement -> statement.getSubject().stringValue().startsWith(prefix))
                .collect(toImmutableList());
    }
}
