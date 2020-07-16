package org.wikidata.query.rdf.mwoauth;

import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.status;
import static javax.ws.rs.core.Response.temporaryRedirect;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
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

import com.github.scribejava.apis.MediaWikiApi;
import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.model.OAuth1AccessToken;
import com.github.scribejava.core.model.OAuth1RequestToken;
import com.github.scribejava.core.oauth.OAuth10aService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

//Multiple classes and annotations from both auth and jax-rs are needed,
// I don't see much point in splitting the service
@SuppressWarnings({"checkstyle:classfanoutcomplexity"})
@Path("/oauth")
@Singleton
public class OAuthProxyService {

    public static final String SESSION_COOKIE_NAME = "wcqsSession";
    public static final int SESSION_MAX_AGE = Integer.MAX_VALUE;

    @Context
    private ServletConfig servletConfig;

    private OAuth10aService service;
    private Cache<String, OAuth1RequestToken> requestTokens;
    private Cache<String, OAuth1AccessToken> accessTokens;
    private String wikiLogoutLink;

    public OAuthProxyService() {}

    @VisibleForTesting
    OAuthProxyService(OAuth10aService oauthService, int sessionStoreLimit, String wikiLogoutLink) {
        requestTokens = buildCache(sessionStoreLimit);
        accessTokens = buildCache(sessionStoreLimit);
        service = oauthService;
        this.wikiLogoutLink = wikiLogoutLink;
    }

    @PostConstruct
    public void init() {
        OAuthProxyConfig oauthProxyConfig = new OAuthProxyConfig(servletConfig);
        requestTokens = buildCache(oauthProxyConfig.sessionStoreLimit());
        accessTokens = buildCache(oauthProxyConfig.sessionStoreLimit());
        wikiLogoutLink = oauthProxyConfig.wikiLogoutLink();
        service = new ServiceBuilder(oauthProxyConfig.consumerKey())
                .apiSecret(oauthProxyConfig.consumerSecret())
                .build(new MediaWikiApi(oauthProxyConfig.indexUrl(), oauthProxyConfig.niceUrlBase()));
    }

    @GET
    @Path("/check_login")
    public Response checkLogin() throws InterruptedException, ExecutionException, IOException, URISyntaxException {
        final OAuth1RequestToken requestToken = service.getRequestToken();
        requestTokens.put(requestToken.getToken(), requestToken);
        String authorizationUrl = service.getAuthorizationUrl(requestToken);

        return temporaryRedirect(getAuthenticationURI(authorizationUrl)).build();
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
        accessTokens.put(accessToken.getToken(), accessToken);
        NewCookie sessionCookie = new NewCookie(SESSION_COOKIE_NAME,
                accessToken.getToken(), "/", null, null, SESSION_MAX_AGE, true, true);

        return temporaryRedirect(new URI(redirectUrl)).cookie(sessionCookie).build();
    }

    @GET
    @Path("/check_auth")
    public Response checkUser(@CookieParam(SESSION_COOKIE_NAME) String session) {
        if (checkSession(session)) {
            return Response.ok().build();
        } else {
            return status(FORBIDDEN).build();
        }
    }

    @GET
    @Path("/logout")
    public Response logout(@CookieParam(SESSION_COOKIE_NAME) String session) throws URISyntaxException {
        if (checkSession(session)) {
            accessTokens.invalidate(session);
        }
        return temporaryRedirect(new URI(wikiLogoutLink))
                .cookie(new NewCookie(new Cookie(SESSION_COOKIE_NAME, "deleted"),
                        "", 0, new Date(0), true, true))
                .build();
    }

    private boolean checkSession(@CookieParam(SESSION_COOKIE_NAME) String session) {
        return session != null && accessTokens.getIfPresent(session) != null;
    }

    private <T> Cache<String, T> buildCache(int maximumSize) {
        return CacheBuilder.newBuilder()
                .maximumSize(maximumSize)
                .build();
    }

    // ScribeJava doesn't not provide mw-oauth authentication url (it's not part of oauth1.0a standard),
    // which is used for authentication only consumers. Fortunately, url is almost the same.
    private URI getAuthenticationURI(String authorizationUrl) throws URISyntaxException {
        return new URI(authorizationUrl.replace("authorize", "authenticate"));
    }
}
