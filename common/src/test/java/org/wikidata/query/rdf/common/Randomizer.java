package org.wikidata.query.rdf.common;

import java.security.SecureRandom;
import java.util.Random;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Minimal replacement of Randomized Testing.
 *
 * This is a simplified copy of the similar class in the testTools module.
 * Duplication is sadly necessary to avoid module cycles.
 */
@NotThreadSafe
public class Randomizer implements TestRule {

    private static final String SEED_PROPERTY = Randomizer.class.getName() + ".seed";
    private static final Logger LOG = LoggerFactory.getLogger(Randomizer.class);
    private Random random;
    private int seed;

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            @SuppressWarnings("IllegalCatch") // we're rethrowing this Throwable immediately
            public void evaluate() throws Throwable {
                before();
                try {
                    base.evaluate();
                } catch (Throwable t) {
                    onError();
                    throw t;
                }
            }
        };
    }

    /**
     * Initialize the random generator.
     */
    private void before() {
        seed = getSeed();
        random = new Random(seed);
    }

    /**
     * Log seed on errors.
     */
    private void onError() {
        LOG.info("Ramdomizer initialized with: [{}], you can override it by setting the system property {}.", seed, SEED_PROPERTY);
    }

    private int getSeed() {
        String seedString = System.getProperty(SEED_PROPERTY);
        if (seedString != null) return Integer.parseInt(seedString);
        return new SecureRandom().nextInt();
    }

    /**
     * A random integer from <code>min</code> to <code>max</code> (inclusive).
     */
    public int randomIntBetween(int min, int max) {
        assert max >= min : "max must be >= min: " + min + ", " + max;
        long range = (long) max - (long) min;
        if (range < Integer.MAX_VALUE) {
            return min + random.nextInt(1 + (int) range);
        } else {
            return min + (int) Math.round(random.nextDouble() * range);
        }
    }

}
