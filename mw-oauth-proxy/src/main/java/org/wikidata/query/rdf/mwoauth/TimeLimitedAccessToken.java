package org.wikidata.query.rdf.mwoauth;

import java.time.Clock;
import java.time.Duration;
import java.util.Date;
import java.util.Set;
import java.util.function.Supplier;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTVerificationException;
import com.auth0.jwt.interfaces.Claim;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.google.common.annotations.VisibleForTesting;

/**
 * Models irrevocable tokens that validate for a limited time span.
 *
 * The token carries a username inside the token, allowing a set of banned usernames
 * to be applied.
 */
class TimeLimitedAccessToken {
    private static final String USERNAME = "username";

    private final Algorithm algorithm;
    private final JWTVerifier verifier;
    private final Duration expireAfter;
    private final Clock clock;
    private final Set<String> bannedUsernames;

    TimeLimitedAccessToken(String secret, Duration expireAfter, Set<String> bannedUsernames) {
        this(Algorithm.HMAC256(secret), expireAfter, bannedUsernames);
    }

    TimeLimitedAccessToken(Algorithm algo, Duration expireAfter, Set<String> bannedUsernames) {

        this(algo, JWT.require(algo).withClaimPresence(USERNAME).build(), expireAfter, bannedUsernames, Clock.systemUTC());
    }

    @VisibleForTesting
    TimeLimitedAccessToken(
        Algorithm algorithm, JWTVerifier verifier, Duration expireAfter,
        Set<String> bannedUsernames, Clock clock
    ) {
        this.algorithm = algorithm;
        this.verifier = verifier;
        this.expireAfter = expireAfter;
        this.clock = clock;
        this.bannedUsernames = bannedUsernames;
    }

    private Date expireAt() {
        return Date.from(clock.instant().plus(expireAfter));
    }

    String create(String username) {
        return JWT.create()
            .withExpiresAt(expireAt())
            .withClaim(USERNAME, username)
            .sign(algorithm);
    }

    <T> T decide(String token, Supplier<T> good, Supplier<T> bad) {
        if (token == null) {
            return bad.get();
        }
        DecodedJWT decoded;
        try {
            decoded = verifier.verify(token);
        } catch (JWTVerificationException e) {
            return bad.get();
        }
        Claim claim = decoded.getClaim(USERNAME);
        if (claim.isNull()) {
            throw new IllegalStateException(("All valid jwt tokens must have a username claim"));
        }
        if (bannedUsernames.contains(claim.asString())) {
           return bad.get();
        }
        return good.get();
    }
}
