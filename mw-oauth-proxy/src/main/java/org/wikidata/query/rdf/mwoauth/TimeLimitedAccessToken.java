package org.wikidata.query.rdf.mwoauth;

import java.time.Clock;
import java.time.Duration;
import java.util.Date;
import java.util.function.Supplier;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTVerificationException;
import com.google.common.annotations.VisibleForTesting;

/**
 * Models irrevocable tokens that validate for a limited time span.
 */
class TimeLimitedAccessToken {
    private final Algorithm algorithm;
    private final JWTVerifier verifier;
    private final Duration expireAfter;
    private final Clock clock;

    TimeLimitedAccessToken(String secret, Duration expireAfter) {
        this(Algorithm.HMAC256(secret), expireAfter);
    }

    TimeLimitedAccessToken(Algorithm algo, Duration expireAfter) {
        this(algo, JWT.require(algo).build(), expireAfter, Clock.systemUTC());
    }

    @VisibleForTesting
    TimeLimitedAccessToken(Algorithm algorithm, JWTVerifier verifier, Duration expireAfter, Clock clock) {
        this.algorithm = algorithm;
        this.verifier = verifier;
        this.expireAfter = expireAfter;
        this.clock = clock;
    }

    private Date expireAt() {
        return Date.from(clock.instant().plus(expireAfter));
    }

    String create() {
        return JWT.create()
            .withExpiresAt(expireAt())
            .sign(algorithm);
    }

    <T> T decide(String token, Supplier<T> good, Supplier<T> bad) {
        if (token == null) {
            return bad.get();
        }
        try {
            verifier.verify(token);
            return good.get();
        } catch (JWTVerificationException e) {
            return bad.get();
        }
    }
}
