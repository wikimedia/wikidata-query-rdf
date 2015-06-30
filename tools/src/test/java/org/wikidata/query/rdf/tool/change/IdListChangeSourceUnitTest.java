package org.wikidata.query.rdf.tool.change;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.wikidata.query.rdf.tool.change.IdListChangeSource.forItems;

import org.junit.Test;
import org.wikidata.query.rdf.tool.exception.RetryableException;

public class IdListChangeSourceUnitTest {
    @Test
    public void empty() throws RetryableException {
        IdListChangeSource changeSource = forItems(new String[0], 10);
        assertEquals(0, changeSource.firstBatch().advanced());
        assertTrue(changeSource.firstBatch().last());
    }

    @Test
    public void one() throws RetryableException {
        IdListChangeSource changeSource = forItems(new String[] {"test"}, 10);
        IdListChangeSource.Batch batch = changeSource.firstBatch();
        assertEquals(1, batch.advanced());
        assertTrue(batch.last());
        assertEquals("0", batch.leftOffHuman());
    }

    @Test
    public void multi() throws RetryableException {
        IdListChangeSource changeSource = forItems(new String[] {"test1", "test2"}, 10);
        IdListChangeSource.Batch batch = changeSource.firstBatch();
        assertEquals(2, batch.advanced());
        assertTrue(batch.last());
        assertEquals("1", batch.leftOffHuman());
    }

    @Test
    public void multiBatch() throws RetryableException {
        IdListChangeSource changeSource = forItems(new String[] {"test1", "test2"}, 1);
        IdListChangeSource.Batch batch = changeSource.firstBatch();
        assertEquals(1, batch.advanced());
        assertFalse(batch.last());
        assertEquals("0", batch.leftOffHuman());
    }
}
