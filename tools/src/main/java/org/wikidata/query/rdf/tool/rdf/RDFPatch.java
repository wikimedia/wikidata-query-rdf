package org.wikidata.query.rdf.tool.rdf;

import java.util.List;

import org.openrdf.model.Statement;

import lombok.Value;

@Value
public class RDFPatch {
    List<Statement> added;
    List<Statement> linkedSharedElements;
    List<Statement> removed;
    List<Statement> unlinkedSharedElements;
}
