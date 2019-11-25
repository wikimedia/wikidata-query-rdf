package org.wikidata.query.rdf.common;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import com.codahale.metrics.Counter;
import com.google.common.annotations.VisibleForTesting;

/**
 * Simple wrapper around a Counter metric.
 * It allows to record times spent using the try-with-resource syntax:
 * <pre>
 * try (Context ctx : counter.time()) {
 *     // process to measure time of
 * }
 * </pre>
 */
public class TimerCounter {

    private final LongConsumer counter;
    private final TimeUnit counterUnit;
    private final TimeUnit clockUnit;
    private final LongSupplier clock;

    @VisibleForTesting
    TimerCounter(LongConsumer counter, TimeUnit counterUnit, LongSupplier clock, TimeUnit clockUnit) {
        this.counter = counter;
        this.counterUnit = counterUnit;
        this.clock = clock;
        this.clockUnit = clockUnit;
    }

    /**
     * Create a new TimerCounter reporting times as milliseconds on top of this Counter.
     */
    public static TimerCounter counter(Counter counter) {
        return new TimerCounter(counter::inc, MILLISECONDS, System::nanoTime, NANOSECONDS);
    }

    public Context time() {
        return new Context(counter, counterUnit, clock, clockUnit);
    }

    public <V> V time(Supplier<V> supplier) {
        try (Context ctx = time()) {
            return supplier.get();
        }
    }

    public void time(Runnable runnable) {
        try (Context ctx = time()) {
            runnable.run();
        }
    }

    public <V, E extends Throwable> V timeCheckedCallable(CheckedCallable<V, E> callable) throws E {
        try (Context ctx = time()) {
            return callable.call();
        }
    }

    public static class Context implements AutoCloseable {
        private final LongConsumer counter;
        private final LongSupplier clock;
        private final TimeUnit counterUnit;
        private final TimeUnit clockUnit;
        private final long start;

        public Context(LongConsumer counter, TimeUnit counterUnit, LongSupplier clock, TimeUnit clockUnit) {
            this.counter = counter;
            this.counterUnit = counterUnit;
            this.clock = clock;
            this.start = clock.getAsLong();
            this.clockUnit = clockUnit;
        }

        @Override
        public void close() {
            counter.accept(counterUnit.convert(clock.getAsLong() - start, clockUnit));
        }
    }

    @FunctionalInterface
    public interface CheckedCallable<V, E extends Throwable> {
        V call() throws E;
    }
}
