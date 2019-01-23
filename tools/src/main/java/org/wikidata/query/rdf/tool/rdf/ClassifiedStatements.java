package org.wikidata.query.rdf.tool.rdf;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

import org.openrdf.model.Statement;
import org.wikidata.query.rdf.common.uri.WikibaseUris;

/**
 * Sort statements into a set of specialized collections, by subject.
 *
 * Multiple calls to {@code classify()} will accumulate statements in this class.
 */
@NotThreadSafe
public class ClassifiedStatements {

    /** Subject is entity. */
    public final List<Statement> statementStatements = new ArrayList<>();
    /** Subject is any statement. */
    public final List<Statement> entityStatements = new ArrayList<>();
    /** Not entity, not statement, not value and not reference. */
    public final Set<Statement> aboutStatements = new HashSet<>();
    private final WikibaseUris uris;

    private long dataSize;

    public ClassifiedStatements(WikibaseUris uris) {
        this.uris = uris;
    }

    public void clear() {
        entityStatements.clear();
        aboutStatements.clear();
        statementStatements.clear();
        dataSize = 0;
    }

    /**
     * Sort statements into a set of specialized collections, by subject.
     *
     * @param statements List of statements to process
     * @param entityId Entity identifier (e.g. Q-id)
     */
    public void classify(Collection<Statement> statements, String entityId) {
        for (Statement statement: statements) {
            String subject = statement.getSubject().stringValue();
            if (subject.equals(uris.entity() + entityId)) {
                entityStatements.add(statement);
            }
            if (subject.startsWith(uris.statement())) {
                statementStatements.add(statement);
            }
            if (!subject.equals(uris.entity() + entityId)
                    && !subject.startsWith(uris.statement())
                    && !subject.startsWith(uris.value())
                    && !subject.startsWith(uris.reference())
                    && !subject.startsWith(uris.entity() + entityId + "-")
            ) {
                aboutStatements.add(statement);
            }
            dataSize += subject.length() + statement.getPredicate().stringValue().length() + statement.getObject().stringValue().length();
        }
    }

    public long getDataSize() {
        return dataSize;
    }
}
