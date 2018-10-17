package org.wikidata.query.rdf.common;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * A {@link TestRule} to run test multiple times.
 *
 * Test methods annotated with {@link @Repeat} will be run the specified number
 * of times. The class under test needs to have a RepeatRule declared as a rule.
 *
 * Note: this can be replaced with RepeatedTest once we upgrade to junit 5.
 * See https://junit.org/junit5/docs/5.0.1/api/org/junit/jupiter/api/RepeatedTest.html
 */
public class RepeatRule implements TestRule {

    @Retention(RUNTIME)
    @Target(METHOD)
    public @interface Repeat {
        int times();
    }

    private static final class RepeatStatement extends Statement {

        private final int times;
        private final Statement statement;

        private RepeatStatement(int times, Statement statement) {
            this.times = times;
            this.statement = statement;
        }

        @Override
        public void evaluate() throws Throwable {
            for (int i = 0; i < times; i++) {
                statement.evaluate();
            }
        }
    }

    @Override
    public Statement apply(Statement statement, Description description) {
        Repeat repeat = description.getAnnotation(Repeat.class);

        if (repeat == null) return statement;

        int times = repeat.times();
        return new RepeatStatement(times, statement);
    }

}
