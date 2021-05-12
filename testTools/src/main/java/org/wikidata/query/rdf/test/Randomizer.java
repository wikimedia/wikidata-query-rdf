package org.wikidata.query.rdf.test;

import java.security.SecureRandom;
import java.util.Random;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;


/**
 * Minimal replacement of Randomized Testing.
 */
@NotThreadSafe
@SuppressFBWarnings(value = "DMI_RANDOM_USED_ONLY_ONCE", justification = "We don't care much about micro optimization here")
public class Randomizer implements TestRule {

    private static final String SEED_PROPERTY = Randomizer.class.getName() + ".seed";
    private static final Logger log = LoggerFactory.getLogger(Randomizer.class);
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
        log.info("Ramdomizer initialized with: [{}], you can override it by setting the system property {}.", seed, SEED_PROPERTY);
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

    /**
     * A random integer between 0 and <code>max</code> (inclusive).
     */
    public int randomInt(int max) {
        if (max == 0)
            return 0;
        else if (max == Integer.MAX_VALUE)
            return random.nextInt() & 0x7fffffff;
        else
            return random.nextInt(max + 1);
    }

    public boolean randomBoolean() {
        return random.nextBoolean();
    }

    public int randomInt() {
        return random.nextInt();
    }

    /**
     * Creates a random string whose length is the number of characters
     * specified.
     *
     * Characters will be chosen from the set of alphabetic
     * characters.
     *
     * Note that this is based on {@link RandomStringUtils} which does not
     * support providing a source of randomness. So this method does not
     * use the same seed as other methods in this class.
     */
    public String randomAsciiOfLength(int count) {
        return RandomStringUtils.randomAlphabetic(count);
    }

    /**
     * Rarely returns <code>true</code> in about 10% of all calls.
     */
    public boolean rarely() {
        return randomInt(100) >= 90;
    }

    public Random getRandom() {
        return random;
    }
}
