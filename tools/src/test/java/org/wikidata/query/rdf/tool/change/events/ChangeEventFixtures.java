package org.wikidata.query.rdf.tool.change.events;

import java.time.Duration;
import java.time.Instant;

/**
 * Provides fixtures to help test classes depending on ChangeEvent.
 */
public final class ChangeEventFixtures {

    private ChangeEventFixtures() {
        // Utility class, should never be constructed
    }

    /**
     * An arbitrary time reference.
     *
     * Events are created with a date that is an offset from this reference.
     *
     * If you are curious, this arbitrary date is 2018-02-09T20:12:33Z, but you
     * should not rely on this in your tests.
     */
    public static final Instant START_TIME = Instant.ofEpochMilli(1518207153000L);
    public static final String DOMAIN = "acme.test";

    /**
     * Make valid RC event.
     *
     * @param offset Duration from start time to event's time
     * @param revid Revision ID
     * @param qid Title (Q-id)
     */
    public static ChangeEvent makeRCEvent(Duration offset, long revid, String qid) {
        return makeRCEvent(offset, revid, qid, 0, DOMAIN);
    }

    /**
     * Make a delete RC event.
     * @param offset Duration from start time to event's time
     * @param qid Title (Q-id)
     */
    public static ChangeEvent makeDeleteEvent(Duration offset, String qid) {
        return new PageDeleteEvent(
                new EventsMeta(START_TIME.plus(offset), "", DOMAIN),
                qid, 0);
    }

    /**
     * Make RC event with different namespace and domain.
     * @param offset Duration from start time to event's time
     * @param revid Revision ID
     * @param qid Title (Q-id)
     */
    public static ChangeEvent makeRCEvent(Duration offset, long revid, String qid, int ns, String domain) {
        return new RevisionCreateEvent(
                new EventsMeta(START_TIME.plus(offset), "", domain),
                revid, qid, ns);
    }
}
