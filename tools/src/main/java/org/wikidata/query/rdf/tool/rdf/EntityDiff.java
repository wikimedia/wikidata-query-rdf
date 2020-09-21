package org.wikidata.query.rdf.tool.rdf;

import static java.util.Collections.unmodifiableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.openrdf.model.Statement;
import org.wikidata.query.rdf.common.uri.UrisScheme;

import com.google.common.collect.Sets;

/**
 * Very naive diff algorithm.
 */
public class EntityDiff {
    private final Predicate<Statement> sharedElementPredicate;

    public EntityDiff(Predicate<Statement> sharedElementPredicate) {
        this.sharedElementPredicate = sharedElementPredicate;
    }

    /**
     * Builds an EntityDiff with shared elements.
     * - declaration of values
     * - declaration of references
     * - declaration of wiki groups (used by sitelink)
     *
     * @see NamespaceStatementPredicates#objectInValueNS(Statement)
     * @see NamespaceStatementPredicates#objectInReferenceNS(Statement)
     * @see StatementPredicates#wikiGroupDefinition(Statement)
     */
    public static EntityDiff withWikibaseSharedElements(UrisScheme scheme) {
        NamespaceStatementPredicates nsStmtPreds = new NamespaceStatementPredicates(scheme);
        return new EntityDiff(stmt -> nsStmtPreds.subjectInReferenceNS(stmt) || nsStmtPreds.subjectInValueNS(stmt)
                || StatementPredicates.wikiGroupDefinition(stmt));
    }

    /**
     * Diff two list of statements.
     */
    public Patch diff(Iterable<Statement> current, Iterable<Statement> next) {
        Set<Statement> currentSet = Sets.newHashSet(current);
        Set<Statement> nextSet = Sets.newHashSet(next);
        Set<Statement> allDeleted = Sets.difference(currentSet, nextSet);
        Set<Statement> allAdded = Sets.difference(nextSet, currentSet);
        List<Statement> added = new ArrayList<>(allAdded.size());
        List<Statement> linkedSharedElements = new ArrayList<>(allAdded.size());
        List<Statement> deleted = new ArrayList<>(allDeleted.size());
        List<Statement> unlinkedSharedElements = new ArrayList<>(allDeleted.size());

        allAdded.forEach(filterSharedElements(linkedSharedElements::add, added::add));
        allDeleted.forEach(filterSharedElements(unlinkedSharedElements::add, deleted::add));
        return new Patch(unmodifiableList(added), unmodifiableList(linkedSharedElements),
                unmodifiableList(deleted), unmodifiableList(unlinkedSharedElements));
    }

    private Consumer<Statement> filterSharedElements(Consumer<Statement> sharedElements, Consumer<Statement> otherElements) {
        return stmt -> {
            if (sharedElementPredicate.test(stmt)) {
                sharedElements.accept(stmt);
            } else {
                otherElements.accept(stmt);
            }
        };
    }

}
