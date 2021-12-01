package org.wikidata.query.rdf.tool.change.events;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Accessors(fluent = true)
public class RevisionSlot {
    String revSlotContentModel;
    String revSlotSha1;
    long revSlotSize;
    Long revSlotOriginRevId;

    @JsonCreator
    public RevisionSlot(
            @JsonProperty("rev_slot_content_model") String revSlotContentModel,
            @JsonProperty("rev_slot_sha1") String revSlotSha1,
            @JsonProperty("rev_slot_size") long revSlotSize,
            @JsonProperty("rev_slot_origin_rev_id") Long revSlotOriginRevId) {
        this.revSlotContentModel = revSlotContentModel;
        this.revSlotSha1 = revSlotSha1;
        this.revSlotSize = revSlotSize;
        this.revSlotOriginRevId = revSlotOriginRevId;
    }
}
