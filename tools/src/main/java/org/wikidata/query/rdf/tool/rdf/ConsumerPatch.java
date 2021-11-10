package org.wikidata.query.rdf.tool.rdf;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.openrdf.model.Statement;

import lombok.Value;

@Value
public class ConsumerPatch {
    List<Statement> added;
    List<Statement> linkedSharedElements;
    List<Statement> removed;
    List<Statement> unlinkedSharedElements;
    List<String> entityIdsToDelete;
    Map<String, Collection<Statement>> reconciliations;

    public String toString() {
        return "ConsumerPatch(added=" + added.size() +
                ", linkedSharedElements=" + linkedSharedElements.size() +
                ", removed=" + removed.size() +
                ", unlinkedSharedElements=" + unlinkedSharedElements.size() +
                ", entityIdsToDelete=" + entityIdsToDelete.size() +
                ", reconciliations=" + reconciliations.size() + ")";
    }
}
