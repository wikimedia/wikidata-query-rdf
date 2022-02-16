package org.wikidata.query.rdf.mwoauth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Date;
import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.Clock;

@RunWith(MockitoJUnitRunner.class)
public class TimeLimitedAccessTokenFactoryUnitTest {
    private final Algorithm algo = Algorithm.HMAC256("not_secret");
    private final Duration duration = Duration.ofMinutes(1);
    private final java.time.Clock epoch = java.time.Clock.fixed(Instant.ofEpochSecond(0), ZoneId.of("UTC"));

    @Test
    public void generatesValidToken() {
        TimeLimitedAccessTokenFactory model = buildModel(duration.getSeconds() - 1);
        assertThat(isValid(model, model.create(""))).isTrue();
    }

    @Test
    public void bannedUsernamesAreRejected() {
        TimeLimitedAccessTokenFactory model = buildModel(
            duration.getSeconds() - 1, Collections.singleton("banned"));
        assertThat(isValid(model, model.create("not_banned"))).isTrue();
        assertThat(isValid(model, model.create("banned"))).isFalse();
    }

    @Test
    public void nullTokenIsRejected() {
        TimeLimitedAccessTokenFactory model = buildModel(duration.getSeconds() - 1);
        assertThat(isValid(model, null)).isFalse();
    }

    @Test
    public void arbitraryTokenIsRejected() {
        TimeLimitedAccessTokenFactory model = buildModel(duration.getSeconds() - 1);
        assertThat(isValid(model, "invalid token")).isFalse();
    }

    @Test
    public void tokenInvalidatesAfterDuration() {
        TimeLimitedAccessTokenFactory model = buildModel(duration.getSeconds() + 1);
        assertThat(isValid(model, model.create(""))).isFalse();
    }

    private TimeLimitedAccessTokenFactory buildModel(long verifyAtEpochSecond) {
        return buildModel(verifyAtEpochSecond, Collections.emptySet());
    }

    private TimeLimitedAccessTokenFactory buildModel(long verifyAtEpochSecond, Set<String> bannedUsers) {
        return new TimeLimitedAccessTokenFactory(algo, timeControlledVerifier(verifyAtEpochSecond), duration, bannedUsers, epoch);
    }

    private JWTVerifier timeControlledVerifier(long verifyAtEpochSecond) {
        Clock jwtClock = mock(Clock.class);
        when(jwtClock.getToday()).thenReturn(Date.from(Instant.ofEpochSecond(verifyAtEpochSecond)));
        return ((JWTVerifier.BaseVerification)JWT.require(algo)).build(jwtClock);
    }

    private boolean isValid(TimeLimitedAccessTokenFactory model, String token) {
        return model.<Boolean>decide(token, () -> true, () -> false);
    }
}
