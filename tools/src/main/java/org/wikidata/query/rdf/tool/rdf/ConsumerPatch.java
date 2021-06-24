package org.wikidata.query.rdf.tool.rdf;

import java.util.List;

import org.openrdf.model.Statement;

import lombok.Value;

@Value
public class ConsumerPatch {
    List<Statement> added;
    List<Statement> linkedSharedElements;
    List<Statement> removed;
    List<Statement> unlinkedSharedElements;
    List<String> entityIdsToDelete;

    public String toString() {
        return "ConsumerPatch(added=" + added.size() +
                ", linkedSharedElements=" + linkedSharedElements.size() +
                ", removed=" + removed.size() +
                ", unlinkedSharedElements=" + unlinkedSharedElements.size() +
                ", entityIdsToDelete=" + entityIdsToDelete.size() + ")";
    }
}
