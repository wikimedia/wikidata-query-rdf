package org.wikidata.query.rdf.blazegraph.throttling;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

import java.time.Instant;

import org.junit.Test;

public class ThrottlingFilterUnitTest {

    @Test
    public void banMessageContainsBanEndDate() {
        Instant until = Instant.parse("2007-12-03T10:15:30.00Z");
        String message = ThrottlingFilter.formattedBanMessage(until);

        assertThat(
                message,
                containsString("until 2007-12-03T10:15:30Z, "));
    }

}
