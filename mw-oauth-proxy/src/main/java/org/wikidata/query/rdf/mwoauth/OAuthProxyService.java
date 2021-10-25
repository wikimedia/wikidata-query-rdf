package org.wikidata.query.rdf.mwoauth;

import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.status;
import static javax.ws.rs.core.Response.temporaryRedirect;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import javax.annotation.PostConstruct;
import javax.inject.Singleton;
import javax.servlet.ServletConfig;
import javax.ws.rs.CookieParam;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.NewCookie;
import javax.ws.rs.core.Response;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClients;

import com.github.scribejava.apis.MediaWikiApi;
import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.model.OAuth1AccessToken;
import com.github.scribejava.core.model.OAuth1RequestToken;
import com.github.scribejava.core.oauth.OAuth10aService;
import com.google.common.annotations.VisibleForTesting;

/**
 * Provides an authentication workflow via mediawiki OAuth 1.0
 *
 * Authorizing a request:
 *  - Auth request comes in to /check_auth. Looks for SESSION_COOKIE_NAME in
 *    the http cookies, if available we attempt to validate as a JWT token.
 *    If valid responds 200 ok, all other conditions return 403 not-authorized.
 *
 *  Creating a new authentication token:
 *  - Initial request comes in to /check_login.  The proxy creates a unique
 *    request token, caches it, and then provides the user a redirect to MW
 *    oauth api including the request token.
 *  - MW will, on successful auth, send the user back to /oauth_verify. The proxy
 *    will lookup the cached request token and provide it along with mw provided
 *    params to the oauth service for verification. On success issues an
 *    irrevocable time-limited access token.
 */
//Multiple classes and annotations from both auth and jax-rs are needed,
// I don't see much point in splitting the service
@SuppressWarnings({"checkstyle:classfanoutcomplexity"})
@Path("/oauth")
@Singleton
public class OAuthProxyService {

    public static final String SESSION_COOKIE_NAME = "wcqsSession";
    public static final int SESSION_MAX_AGE = Integer.MAX_VALUE;
    public static final Duration AUTH_TOKEN_MAX_AGE = Duration.ofHours(2);

    @Context
    private ServletConfig servletConfig;

    private OAuth10aService service;
    private OAuthIdentifyService identify;
    private KaskSessionStore<OAuth1RequestToken> requestTokens;
    private String wikiLogoutLink;
    private String sessionKeyPrefix;
    private String successRedirect;
    private TimeLimitedAccessToken authToken;
    private Set<String> bannedUsernames;

    @PostConstruct
    public void init() {
        OAuthProxyConfig oauthConfig = new OAuthProxyConfig(servletConfig);
        OAuth10aService oauthService = new ServiceBuilder(oauthConfig.consumerKey())
            .apiSecret(oauthConfig.consumerSecret())
            .build(new MediaWikiApi(oauthConfig.indexUrl(), oauthConfig.niceUrlBase()));
        KaskSessionStore<OAuth1RequestToken> sessionStore = buildSessionStore(oauthConfig, HttpClients.createDefault());
        OAuthIdentifyService identify = new OAuthIdentifyService(oauthService, oauthConfig.indexUrl(), oauthConfig.consumerSecret());
        init(oauthConfig, oauthService, identify, sessionStore);
    }

    @VisibleForTesting
    public void init(OAuthProxyConfig config, OAuth10aService service, OAuthIdentifyService identify, KaskSessionStore<OAuth1RequestToken> sessionStore) {
        requestTokens = sessionStore;
        sessionKeyPrefix = config.sessionStoreKeyPrefix();
        wikiLogoutLink = config.wikiLogoutLink();
        successRedirect = config.successRedirect();
        bannedUsernames = config.bannedUsernames();
        authToken = new TimeLimitedAccessToken(config.accessTokenSecret(), config.accessTokenDuration(), bannedUsernames);
        this.service = service;
        this.identify = identify;
    }

    @GET
    @Path("/check_login")
    public Response checkLogin() throws InterruptedException, ExecutionException, IOException, URISyntaxException {
        final OAuth1RequestToken requestToken = service.getRequestToken();
        requestTokens.put(requestToken.getToken(), requestToken);
        String authorizationUrl = service.getAuthorizationUrl(requestToken);

        return temporaryRedirect(getAuthenticationURI(authorizationUrl)).build();
    }

    private NewCookie sessionCookie(String value, int maxAge) {
        return new NewCookie(SESSION_COOKIE_NAME, value, "/", null, null, maxAge, true, true);
    }

    @GET
    @Path("/oauth_verify")
    public Response oauthVerify(@QueryParam ("oauth_verifier") String oauthVerifier,
                                @QueryParam ("oauth_token") String oauthToken,
                                @HeaderParam("X-redirect-url") String redirectUrl
    ) throws InterruptedException, ExecutionException, IOException, URISyntaxException {
        OAuth1RequestToken requestToken = requestTokens.getIfPresent(oauthToken);
        if (requestToken == null) {
            return status(FORBIDDEN).build();
        }
        requestTokens.invalidate(oauthToken);
        OAuth1AccessToken accessToken = service.getAccessToken(requestToken, oauthVerifier);
        String username = identify.getUsername(accessToken);
        if (bannedUsernames.contains(username)) {
            return status(FORBIDDEN).build();
        }
        NewCookie cookie = sessionCookie(authToken.create(username), SESSION_MAX_AGE);
        if (redirectUrl == null) {
            redirectUrl = successRedirect;
        }
        return temporaryRedirect(new URI(redirectUrl)).cookie(cookie).build();
    }

    @GET
    @Path("/check_auth")
    public Response checkUser(@CookieParam(SESSION_COOKIE_NAME) String session) {
        return authToken.decide(
            session,
            () -> Response.ok().build(),
            () -> status(FORBIDDEN).build());
    }

    @GET
    @Path("/logout")
    public Response logout(String session) throws URISyntaxException {
        // The token itself can't be expired, all we can do is remove the cookie holding it.
        // If the token was captured somewhere else it is still valid for the token duration.
        // Accordingly, durations should be kept short. Minutes or hours not days or weeks.
        // Re-auth should be transparent and safe to re-check.
        return temporaryRedirect(new URI(wikiLogoutLink))
            .cookie(new NewCookie(new Cookie(SESSION_COOKIE_NAME, "deleted"),
                "", 0, new Date(0), true, true))
            .build();
    }

    // ScribeJava doesn't not provide mw-oauth authentication url (it's not part of oauth1.0a standard),
    // which is used for authentication only consumers. Fortunately, url is almost the same.
    private URI getAuthenticationURI(String authorizationUrl) throws URISyntaxException {
        return new URI(authorizationUrl.replace("authorize", "authenticate"));
    }

    private KaskSessionStore<OAuth1RequestToken> buildSessionStore(OAuthProxyConfig config, HttpClient httpClient) {
        return new KaskSessionStore<>(
            httpClient,
            config.sessionStoreHost(),
            new KaskSessionStore.Serde<OAuth1RequestToken>() {
                @Override
                public String keyEncoder(String key) {
                    return sessionKeyPrefix + key;
                }

                @Override
                public void valueEncoder(DataOutput out, OAuth1RequestToken token) throws IOException {
                    // Is a serialization version necessary? Shouldn't hurt.
                    out.writeShort(0);
                    // Write out constructor args, rather than full object serialization,
                    // to get better guarantees about what we are doing with what we read back.
                    out.writeUTF(token.getToken());
                    out.writeUTF(token.getTokenSecret());
                }

                @Override
                public OAuth1RequestToken valueDecoder(DataInput in) throws IOException {
                    if (in.readShort() != 0) {
                        throw new IllegalStateException("serialization version mismatch");
                    }
                    return new OAuth1RequestToken(in.readUTF(), in.readUTF());
                }
            });
    }

}
