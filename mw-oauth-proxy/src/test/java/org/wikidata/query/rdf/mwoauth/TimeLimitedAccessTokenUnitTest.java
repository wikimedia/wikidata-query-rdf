package org.wikidata.query.rdf.mwoauth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Date;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.Clock;

@RunWith(MockitoJUnitRunner.class)
public class TimeLimitedAccessTokenUnitTest {
    private final Algorithm algo = Algorithm.HMAC256("not_secret");
    private final Duration duration = Duration.ofMinutes(1);
    private final java.time.Clock epoch = java.time.Clock.fixed(Instant.ofEpochSecond(0), ZoneId.of("UTC"));

    @Test
    public void generatesValidToken() {
        TimeLimitedAccessToken model = buildModel(duration.getSeconds() - 1);
        assertThat(isValid(model, model.create())).isTrue();

    }

    @Test
    public void nullTokenIsRejected() {
        TimeLimitedAccessToken model = buildModel(duration.getSeconds() - 1);
        assertThat(isValid(model, null)).isFalse();
    }

    @Test
    public void arbitraryTokenIsRejected() {
        TimeLimitedAccessToken model = buildModel(duration.getSeconds() - 1);
        assertThat(isValid(model, "invalid token")).isFalse();
    }

    @Test
    public void tokenInvalidatesAfterDuration() {
        TimeLimitedAccessToken model = buildModel(duration.getSeconds() + 1);
        assertThat(isValid(model, model.create())).isFalse();
    }

    private TimeLimitedAccessToken buildModel(long verifyAtEpochSecond) {
        return new TimeLimitedAccessToken(algo, timeControlledVerifier(verifyAtEpochSecond), duration, epoch);
    }

    private JWTVerifier timeControlledVerifier(long verifyAtEpochSecond) {
        Clock jwtClock = mock(Clock.class);
        when(jwtClock.getToday()).thenReturn(Date.from(Instant.ofEpochSecond(verifyAtEpochSecond)));
        return ((JWTVerifier.BaseVerification)JWT.require(algo)).build(jwtClock);
    }

    private boolean isValid(TimeLimitedAccessToken model, String token) {
        return model.<Boolean>decide(token, () -> true, () -> false);
    }
}
