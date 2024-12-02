package org.wikidata.query.rdf.updater.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.junit.Test;
import org.wikidata.query.rdf.tool.change.events.EventsMeta;
import org.wikidata.query.rdf.updater.MutationEventData;
import org.wikidata.query.rdf.updater.MutationEventDataV2;

public class StreamingUpdateUnitTest {
    private MutationEventData event(String entity, String domain) {
        return new MutationEventDataV2(
                new EventsMeta(null, null, domain, "null", null),
                entity, 1, Instant.EPOCH, 0, 1, "DELETE");
    }

    @Test
    public void testCanaryEventFilter() {
        Predicate<MutationEventData> pred = StreamingUpdate.canaryEventFilter();
        assertThat(pred.test(event("Q1", "canary"))).isFalse();
        assertThat(pred.test(event("Q1", "not a canary"))).isTrue();
    }

    @Test
    public void testFilter() {
        Predicate<MutationEventData> pred = StreamingUpdate.buildFilter(Pattern.compile("Q1"));
        assertThat(pred.test(event("Q1", "canary"))).isFalse();
        assertThat(pred.test(event("Q1", "not a canary"))).isTrue();

        assertThat(pred.test(event("Q2", "canary"))).isFalse();
        assertThat(pred.test(event("Q2", "not a canary"))).isFalse();
    }
}
