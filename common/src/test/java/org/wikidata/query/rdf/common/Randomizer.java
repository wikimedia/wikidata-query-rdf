package org.wikidata.query.rdf.common;

import java.security.SecureRandom;
import java.util.Random;

import javax.annotation.concurrent.NotThreadSafe;

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
