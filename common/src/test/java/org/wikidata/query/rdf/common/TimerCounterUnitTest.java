package org.wikidata.query.rdf.common;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;

public class TimerCounterUnitTest {
    private TimerCounter counter;
    private ArrayList<Long> recordTimes;

    @Before
    public void init() {
        Iterator<Long> clockTimesNano = Arrays.asList(0L, MILLISECONDS.toNanos(150)).iterator();
        recordTimes = new ArrayList<>();
        counter = new TimerCounter(recordTimes::add, MILLISECONDS, clockTimesNano::next, NANOSECONDS);
    }
    @Test
    @SuppressWarnings("EmptyBlock")
    public void testTryWithResource() {
        try (TimerCounter.Context ctx = counter.time()) {
        }
        assertThat(recordTimes).containsExactly(150L);
    }

    @Test
    public void testTimedSupplier() {
        Object myValue = new Object();
        Object returnedObject = counter.time(() -> myValue);
        assertThat(returnedObject).isSameAs(myValue);
        assertThat(recordTimes).containsExactly(150L);
    }

    @Test
    public void testTimedRunnable() {
        counter.time(() -> {});
        assertThat(recordTimes).containsExactly(150L);
    }

    @Test
    public void testTimedCheckedCallable() {
        Object myValue = new Object();
        Object returnedObject = counter.timeCheckedCallable(() -> myValue);
        assertThat(returnedObject).isSameAs(myValue);
        assertThat(recordTimes).containsExactly(150L);
    }

    @Test
    public void testTimedThrowingCheckedCallable() {
        assertThatThrownBy(() -> counter.timeCheckedCallable(() -> {
            throw new IllegalArgumentException("boom");
        })).isInstanceOf(IllegalArgumentException.class);
        assertThat(recordTimes).containsExactly(150L);
    }
}
