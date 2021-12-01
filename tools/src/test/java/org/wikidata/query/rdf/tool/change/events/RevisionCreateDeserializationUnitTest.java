package org.wikidata.query.rdf.tool.change.events;


import static org.assertj.core.api.Assertions.assertThat;
import static com.google.common.io.Resources.getResource;

import java.io.IOException;
import java.net.URL;

import org.assertj.core.data.MapEntry;
import org.junit.Test;
import org.wikidata.query.rdf.tool.MapperUtils;

public class RevisionCreateDeserializationUnitTest {

    @Test
    public void shouldDeserializeCorrectly() throws IOException {
        URL resource = getResource("org/wikidata/query/rdf/tool/change/events/create-event-full.json");
        RevisionCreateEvent revisionCreateEvent = MapperUtils.getObjectMapper().readValue(resource, RevisionCreateEvent.class);

        assertThat(revisionCreateEvent.pageId()).isEqualTo(22419364);
        assertThat(revisionCreateEvent.namespace()).isEqualTo(0);
        assertThat(revisionCreateEvent.title()).isEqualTo("Q20672616");
        RevisionSlot revisionSlot =
                new RevisionSlot("wikitext", "123fewfwe", 1571, 1L);
        assertThat(revisionCreateEvent.revSlots()).containsExactly(MapEntry.entry("main", revisionSlot));
    }
}
