package org.wikidata.query.rdf.updater;

import org.apache.flink.metrics.Counter;

public class DropwizardCounterWrapper implements Counter {

    private final com.codahale.metrics.Counter counter;

    public DropwizardCounterWrapper(com.codahale.metrics.Counter counter) {

        this.counter = counter;
    }

    @Override
    public void inc() {
        counter.inc();
    }

    @Override
    public void inc(long n) {
        counter.inc(n);
    }

    @Override
    public void dec() {
        counter.dec();
    }

    @Override
    public void dec(long n) {
        counter.dec(n);
    }

    @Override
    public long getCount() {
        return counter.getCount();
    }
}
