package org.wikidata.query.rdf.updater;

import java.io.Serializable;

public interface MutationEventData extends Serializable {
    String SCHEMA_TITLE = "mediawiki/wikibase/entity/rdf_change";
    String DIFF_OPERATION = "diff";
    String IMPORT_OPERATION = "import";
    String DELETE_OPERATION = "delete";
    String RECONCILE_OPERATION = "reconcile";

    String getSchema();

    org.wikidata.query.rdf.tool.change.events.EventsMeta getMeta();

    String getEntity();

    long getRevision();

    java.time.Instant getEventTime();

    int getSequence();

    int getSequenceLength();

    String getOperation();
}
