package org.wikidata.query.rdf.blazegraph;

import static org.hamcrest.Matchers.lessThan;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.RandomizedTest;

@RunWith(RandomizedRunner.class)
public class DummyUnitTest extends RandomizedTest {
    @Test
    public void dummy() {
        // TODO remove me when there are real tests here
        assertThat(randomIntBetween(0, 10), lessThan(11));
    }
}
