package org.wikidata.query.rdf.blazegraph.filters;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTVerificationException;
import com.auth0.jwt.interfaces.DecodedJWT;

/**
 * Overrides getRemoteUser() for requests containing a signed JWT token containing a claim of the identity.
 *
 * The token must be created and provided as a cookie to the user in some external service,
 * such as mw-oauth-proxy. The provided identity claim must be represented as a string.
 */
public class JWTIdentityFilter implements Filter {

    private static final Logger log = LoggerFactory.getLogger(JWTIdentityFilter.class);

    private Function<HttpServletRequest, Optional<String>> usernames;

    private Function<HttpServletRequest, Optional<String>> usernameProvider(FilterConfiguration config) {
        String cookieName = config.loadStringParam("jwt-identity-cookie-name");
        String identityClaim = config.loadStringParam("jwt-identity-claim");
        String secret = config.loadStringParam("jwt-identity-secret");
        if (allNotNull(cookieName, identityClaim, secret)) {
            log.info("Configured filter against {} claim of jwt token in the {} cookie", identityClaim, cookieName);
            return new UsernameFromJWTCookie(cookieName, identityClaim, secret);
        } else if (anyNotNull(cookieName, identityClaim, secret)) {
            throw new IllegalArgumentException(
                "All three of jwt-identity-cookie-name, jwt-identity-claim, and jwt-identity-secret " +
                    "must be provided");
        } else {
            log.info("Filter disabled, no configuration available.");
            // Better way to disable filter when unconfigured? Seems better than returning a null provider.
            return r -> Optional.empty();
        }
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        usernames = usernameProvider(new FilterConfiguration(filterConfig, FilterConfiguration.WDQS_CONFIG_PREFIX));
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        filterChain.doFilter(ProvidedRemoteUserHttpRequestWrapper.wrap(servletRequest, usernames), servletResponse);
    }

    @Override
    public void destroy() {
    }

    private boolean anyNotNull(Object... values) {
        return Arrays.stream(values).anyMatch(Objects::nonNull);
    }

    private boolean allNotNull(Object... values) {
        return Arrays.stream(values).allMatch(Objects::nonNull);
    }

    public static class UsernameFromJWTCookie implements Function<HttpServletRequest, Optional<String>> {
        private final String cookieName;
        private final String identityClaim;
        private final JWTVerifier verifier;

        public UsernameFromJWTCookie(String cookieName, String identityClaim, String secret) {
            this.cookieName = cookieName;
            this.identityClaim = identityClaim;
            this.verifier = JWT.require(Algorithm.HMAC256(secret)).withClaimPresence(identityClaim).build();
        }

        public Optional<String> apply(HttpServletRequest request) {
            return Optional.ofNullable(request.getCookies())
                .map(Arrays::stream)
                .orElseGet(Stream::empty)
                .filter(c -> cookieName.equals(c.getName()))
                .findFirst()
                .map(Cookie::getValue)
                .flatMap(this::decode)
                .flatMap(t -> Optional.ofNullable(t.getClaim(identityClaim).asString()));
        }

        private Optional<DecodedJWT> decode(String token) {
            try {
                return Optional.ofNullable(verifier.verify(token));
            } catch (JWTVerificationException e) {
                log.info("Received invalid JWT token, incorrect secret?");
                return Optional.empty();
            }
        }
    }

    private static final class ProvidedRemoteUserHttpRequestWrapper extends HttpServletRequestWrapper {
        private final String remoteUser;

        public static ServletRequest wrap(ServletRequest servletRequest, Function<HttpServletRequest, Optional<String>> provider) {
            try {
                return wrap((HttpServletRequest)servletRequest, provider);
            } catch (ClassCastException e) {
                // In practice should be unreachable? Unclear.
                return servletRequest;
            }
        }

        public static HttpServletRequest wrap(HttpServletRequest servletRequest, Function<HttpServletRequest, Optional<String>> provider) {
            return provider.apply(servletRequest)
                .<HttpServletRequest>map(username -> new ProvidedRemoteUserHttpRequestWrapper(servletRequest, username))
                .orElse(servletRequest);
        }

        ProvidedRemoteUserHttpRequestWrapper(HttpServletRequest request, String remoteUser) {
            super(request);
            this.remoteUser = remoteUser;
        }

        @Override
        public String getRemoteUser() {
            return remoteUser;
        }
    }
}
