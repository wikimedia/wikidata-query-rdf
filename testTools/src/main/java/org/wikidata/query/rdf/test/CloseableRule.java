package org.wikidata.query.rdf.test;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;


/**
 * A {@link TestRule} that will automatically close an object after tests.
 *
 * This makes it slightly nicer than having @After methods for each resource to
 * close. It makes most sense when a test instantiate multiple {@link
 * AutoCloseable} resources.
 *
 * @param <T> The type of the object managed by this rule.
 */
public final class CloseableRule<T extends AutoCloseable> implements TestRule {

    private final T closeable;

    public static <T extends AutoCloseable> CloseableRule<T> autoClose(T closeable) {
        return new CloseableRule<>(closeable);
    }

    private CloseableRule(T closeable) {
        this.closeable = closeable;
    }

    public T get() {
        return closeable;
    }

    /** {@inheritDoc} */
    @Override
    public Statement apply(final Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try {
                    base.evaluate();
                } finally {
                    closeable.close();
                }
            }
        };
    }

}
