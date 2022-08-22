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
import java.util.Optional;
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

import lombok.EqualsAndHashCode;

/**
 * Provides an authentication workflow via mediawiki OAuth 1.0
 *
 * Authorizing a request:
 *  - Auth request comes in to /check_auth. Looks for SESSION_COOKIE_NAME in
 *    the http cookies, if available we attempt to validate as a JWT token.
 *    If valid responds 200 ok, all other conditions return 403 not-authorized.
 *
 *  Creating a new authentication token:
 *  - Initial request comes in to /check_login.
 *  - Look for AUTH_COOKIE_NAME in the http cookies, if available attempt to use
 *    this value to fetch an access token from session storage and sign an identify
 *    request. If valid issue a new JWT token.
 *  - Otherwise, the proxy creates a unique request token, caches it, and then
 *    provides the user a redirect to MW oauth api including the request token.
 *  - MW will, on successful auth, send the user back to /oauth_verify. The proxy
 *    will lookup the cached request token and provide it along with mw provided
 *    params to the oauth service for verification. On success issues an
 *    irrevocable time-limited access token in SESSION_COOKIE_NAME and stores
 *    the oauth access token in AUTH_COOKIE_NAME.
 */
//Multiple classes and annotations from both auth and jax-rs are needed,
// I don't see much point in splitting the service
@SuppressWarnings({"checkstyle:classfanoutcomplexity"})
@Path("/oauth")
@Singleton
public class OAuthProxyService {

    public static final String SESSION_COOKIE_NAME = "wcqsSession";
    public static final String OAUTH_COOKIE_NAME = "wcqsOauth";

    @Context
    private ServletConfig servletConfig;

    private OAuth10aService service;
    private OAuthIdentifyService identify;
    // Sessions that are authenticating with the oauth provider
    private KaskSessionStore<PreAuthSessionState> preAuthSessions;
    // Sessions that have previously authenticated with the oauth provider
    private KaskSessionStore<OAuth1AccessToken> authorizedSessions;
    private String wikiLogoutLink;
    private String sessionKeyPrefix;
    private String successRedirect;
    private TimeLimitedAccessTokenFactory authTokenFactory;
    private Set<String> bannedUsernames;
    private Duration expireAfter;

    @EqualsAndHashCode
    static class PreAuthSessionState {
        public final OAuth1RequestToken token;
        public final Optional<URI> returnUri;

        PreAuthSessionState(OAuth1RequestToken token, Optional<URI> returnUri) {
            this.token = token;
            this.returnUri = returnUri;
        }
    }

    @PostConstruct
    public void init() {
        OAuthProxyConfig oauthConfig = new OAuthProxyConfig(servletConfig);
        OAuth10aService oauthService = new ServiceBuilder(oauthConfig.consumerKey())
            .apiSecret(oauthConfig.consumerSecret())
            .build(new MediaWikiApi(oauthConfig.indexUrl(), oauthConfig.niceUrlBase()));
        HttpClient httpClient = HttpClients.createDefault();
        KaskSessionStore<PreAuthSessionState> requestTokenStore = buildRequestTokenStore(oauthConfig, httpClient);
        KaskSessionStore<OAuth1AccessToken> accessTokenStore = buildAccessTokenStore(oauthConfig, httpClient);
        OAuthIdentifyService identify = new OAuthIdentifyService(oauthService, oauthConfig.indexUrl(), oauthConfig.consumerSecret());
        init(oauthConfig, oauthService, identify, requestTokenStore, accessTokenStore);
    }

    @VisibleForTesting
    public void init(OAuthProxyConfig config, OAuth10aService service, OAuthIdentifyService identify,
                     KaskSessionStore<PreAuthSessionState> requestTokenStore, KaskSessionStore<OAuth1AccessToken> accessTokenStore) {
        preAuthSessions = requestTokenStore;
        authorizedSessions = accessTokenStore;
        sessionKeyPrefix = config.sessionStoreKeyPrefix();
        wikiLogoutLink = config.wikiLogoutLink();
        successRedirect = config.successRedirect();
        bannedUsernames = config.bannedUsernames();
        expireAfter = config.accessTokenDuration();
        authTokenFactory = new TimeLimitedAccessTokenFactory(config.accessTokenSecret(), expireAfter, bannedUsernames);
        this.service = service;
        this.identify = identify;
    }

    @GET
    @Path("/check_login")
    public Response checkLogin(
        @HeaderParam("X-redirect-url") String redirectUrl,
        @CookieParam(OAUTH_COOKIE_NAME) String rawAccessToken
    ) throws InterruptedException, ExecutionException, IOException, URISyntaxException {
        // Step 1: Try to verify rawAccessToken is still valid with the oauth provider by
        // performing a signed request for the identity associated with the token, if so
        // issue a fresh JWT. This avoids sending the user through a round-trip to the oauth
        // service that may fail if we are inside an XHR request (and mw oauth doesn't allow CORS).
        Optional<String> username = Optional.ofNullable(rawAccessToken)
            .map(authorizedSessions::getIfPresent)
            .flatMap(identify::getUsername)
            .filter(name -> !bannedUsernames.contains(name));
        if (username.isPresent()) {
            URI finalRedirect = URI.create(redirectUrl == null ? successRedirect : redirectUrl);
            return temporaryRedirect(finalRedirect)
                .cookie(sessionCookie(authTokenFactory.create(username.get())))
                .build();
        }
        // Step 2: Start an oauth request flow
        final OAuth1RequestToken requestToken = service.getRequestToken();
        preAuthSessions.put(requestToken.getToken(),
            new PreAuthSessionState(requestToken, Optional.ofNullable(redirectUrl).map(URI::create)));
        String authorizationUrl = service.getAuthorizationUrl(requestToken);
        return temporaryRedirect(getAuthenticationURI(authorizationUrl)).build();
    }

    private NewCookie sessionCookie(String value) {
        return new NewCookie(SESSION_COOKIE_NAME, value, "/", null, null, (int) expireAfter.getSeconds(), true, true);
    }

    private NewCookie authTokenCookie(String value) {
        return new NewCookie(OAUTH_COOKIE_NAME, value, "/", null, null, NewCookie.DEFAULT_MAX_AGE, true, true);
    }

    private NewCookie deleteCookie(String name) {
        return new NewCookie(name, "deleted", "/", null, Cookie.DEFAULT_VERSION,
            null, 0, new Date(0), true, true);
    }

    @GET
    @Path("/oauth_verify")
    public Response oauthVerify(@QueryParam ("oauth_verifier") String oauthVerifier,
                                @QueryParam ("oauth_token") String oauthToken,
                                @HeaderParam("X-redirect-url") String redirectUrl
    ) throws InterruptedException, ExecutionException, IOException {
        PreAuthSessionState state = preAuthSessions.getIfPresent(oauthToken);
        if (state == null) {
            return status(FORBIDDEN).build();
        }
        preAuthSessions.invalidate(oauthToken);
        OAuth1AccessToken accessToken = service.getAccessToken(state.token, oauthVerifier);
        Optional<String> username = identify.getUsername(accessToken)
            .filter(name -> !bannedUsernames.contains(name));
        if (!username.isPresent()) {
            return status(FORBIDDEN).build();
        }
        authorizedSessions.put(accessToken.getToken(), accessToken);
        // prefer the uri from initial request, then from current request, and finally the system default.
        URI finalRedirect = state.returnUri.orElseGet(() -> URI.create(redirectUrl != null ? redirectUrl : successRedirect));
        return temporaryRedirect(finalRedirect)
            .cookie(sessionCookie(authTokenFactory.create(username.get())))
            .cookie(authTokenCookie(accessToken.getToken()))
            .build();
    }

    @GET
    @Path("/check_auth")
    public Response checkUser(@CookieParam(SESSION_COOKIE_NAME) String session) {
        return authTokenFactory.decide(
            session,
            () -> Response.ok().build(),
            () -> status(FORBIDDEN).build());
    }

    @GET
    @Path("/logout")
    public Response logout(
        @CookieParam(OAUTH_COOKIE_NAME) String oauthToken
    ) throws URISyntaxException, IOException {
        if (oauthToken != null) {
            authorizedSessions.invalidate(oauthToken);
        }
        return temporaryRedirect(new URI(wikiLogoutLink))
            // The token itself can't be expired, all we can do is remove the cookie holding it.
            // If the token was captured somewhere else it is still valid for the token duration.
            // Accordingly, durations should be kept short. Minutes or hours not days or weeks.
            // Re-auth should be transparent and safe to re-check.
            .cookie(deleteCookie(SESSION_COOKIE_NAME))
            // The auth cookie refers to an entry at the oauth provider, we drop it from our session store but it still
            // exists and is valid at the oauth provider. With the secret forgotten the plain value should be useless.
            .cookie(deleteCookie(OAUTH_COOKIE_NAME))
            .build();
    }

    // ScribeJava doesn't provide an mw-oauth authentication url (it's not part of oauth1.0a standard),
    // which is used for authentication only consumers. Fortunately, url is almost the same.
    private URI getAuthenticationURI(String authorizationUrl) throws URISyntaxException {
        return new URI(authorizationUrl.replace("authorize", "authenticate"));
    }

    @VisibleForTesting
    static KaskSessionStore.Serde<OAuth1AccessToken> oAuth1AccessTokenSerde(String sessionKeyPrefix) {
        return new KaskSessionStore.Serde<OAuth1AccessToken>() {
            @Override
            public String keyEncoder(String key) {
                return sessionKeyPrefix + ":access:" + key;
            }

            @Override
            public void valueEncoder(DataOutput out, OAuth1AccessToken token) throws IOException {
                out.writeShort(0);
                out.writeUTF(token.getToken());
                out.writeUTF(token.getTokenSecret());
            }

            @Override
            public OAuth1AccessToken valueDecoder(DataInput in) throws IOException {
                short version = in.readShort();
                if (version != 0) {
                    throw new IllegalStateException("serialization version mismatch");
                }
                String token = in.readUTF();
                String secret = in.readUTF();
                return new OAuth1AccessToken(token, secret);
            }
        };
    }

    private KaskSessionStore<OAuth1AccessToken> buildAccessTokenStore(OAuthProxyConfig config, HttpClient httpClient) {
        return new KaskSessionStore<>(httpClient, config.sessionStoreHost(), oAuth1AccessTokenSerde(sessionKeyPrefix));
    }

    @VisibleForTesting
    static KaskSessionStore.Serde<PreAuthSessionState> preAuthSessionStateSerde(String sessionKeyPrefix) {
        return new KaskSessionStore.Serde<PreAuthSessionState>() {
            @Override
            public String keyEncoder(String key) {
                return sessionKeyPrefix + ":request:" + key;
            }

            @Override
            public void valueEncoder(DataOutput out, PreAuthSessionState state) throws IOException {
                out.writeShort(1);
                // Write out constructor args, rather than full object serialization,
                // to get better guarantees about what we are doing with what we read back.
                out.writeUTF(state.token.getToken());
                out.writeUTF(state.token.getTokenSecret());
                out.writeBoolean(state.returnUri.isPresent());
                if (state.returnUri.isPresent()) {
                    out.writeUTF(state.returnUri.get().toString());
                }
            }

            @Override
            public PreAuthSessionState valueDecoder(DataInput in) throws IOException {
                short version = in.readShort();
                if (version != 0 && version != 1) {
                    throw new IllegalStateException("serialization version mismatch");
                }
                OAuth1RequestToken token = new OAuth1RequestToken(in.readUTF(), in.readUTF());
                Optional<URI> returnUri = Optional.empty();
                if (version >= 1 && in.readBoolean()) {
                    returnUri = Optional.of(URI.create(in.readUTF()));
                }
                return new PreAuthSessionState(token, returnUri);
            }
        };
    }

    private KaskSessionStore<PreAuthSessionState> buildRequestTokenStore(OAuthProxyConfig config, HttpClient httpClient) {
        return new KaskSessionStore<>(httpClient, config.sessionStoreHost(), preAuthSessionStateSerde(sessionKeyPrefix));
    }

}
