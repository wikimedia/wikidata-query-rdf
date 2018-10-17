package org.wikidata.query.rdf.test;

import java.security.SecureRandom;
import java.util.Random;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Minimal replacement of Randomized Testing.
 */
@NotThreadSafe
public class Randomizer extends ExternalResource {

    private static final String SEED_PROPERTY = Randomizer.class.getName() + ".seed";
    private static final Logger log = LoggerFactory.getLogger(Randomizer.class);
    private Random random;

    /**
     * Initialize the random generator.
     */
    @Override
    protected void before() {
        int seed = getSeed();
        log.info("Ramdomizer initialized with: [{}], you can override it by setting the system property {}.", seed, SEED_PROPERTY);
        random = new Random(seed);
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
