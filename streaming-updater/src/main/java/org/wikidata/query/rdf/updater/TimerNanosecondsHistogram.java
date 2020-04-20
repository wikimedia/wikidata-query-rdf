package org.wikidata.query.rdf.updater;

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

public class TimerNanosecondsHistogram extends Histogram {
    private final Timer timer;

    public TimerNanosecondsHistogram(Timer timer) {
        super(new ExponentiallyDecayingReservoir());
        this.timer = timer;
    }

    @Override
    public void update(int value) {
        timer.update(value, TimeUnit.NANOSECONDS);
    }

    @Override
    public void update(long value) {
        timer.update(value, TimeUnit.NANOSECONDS);
    }

    @Override
    public long getCount() {
        return timer.getCount();
    }

    @Override
    public Snapshot getSnapshot() {
        return timer.getSnapshot();
    }
}
