package org.wikidata.query.rdf.mwoauth;

import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.status;
import static javax.ws.rs.core.Response.temporaryRedirect;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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
import javax.ws.rs.core.Response;

import com.github.scribejava.apis.MediaWikiApi;
import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.model.OAuth1AccessToken;
import com.github.scribejava.core.model.OAuth1RequestToken;
import com.github.scribejava.core.oauth.OAuth10aService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

@Path("/oauth")
@Singleton
public class OAuthProxyService {

    @Context
    private ServletConfig servletConfig;

    private OAuth10aService service;
    private Cache<String, OAuth1RequestToken> requestTokens;
    private Cache<String, OAuth1AccessToken> accessTokens;

    public OAuthProxyService() {}

    @VisibleForTesting
    OAuthProxyService(OAuth10aService oauthService, int sessionStoreLimit) {
        requestTokens = buildCache(sessionStoreLimit);
        accessTokens = buildCache(sessionStoreLimit);
        service = oauthService;
    }

    @PostConstruct
    public void init() {
        OAuthProxyConfig oauthProxyConfig = new OAuthProxyConfig(servletConfig);
        requestTokens = buildCache(oauthProxyConfig.sessionStoreLimit());
        accessTokens = buildCache(oauthProxyConfig.sessionStoreLimit());
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
                                @CookieParam ("wiki_session") String wikiSession,
                                @HeaderParam("X-redirect-url") String redirectUrl
    ) throws InterruptedException, ExecutionException, IOException, URISyntaxException {
        OAuth1RequestToken requestToken = requestTokens.getIfPresent(oauthToken);
        if (requestToken == null) {
            return status(FORBIDDEN).build();
        }
        OAuth1AccessToken accessToken = service.getAccessToken(requestToken, oauthVerifier);
        accessTokens.put(wikiSession, accessToken);
        return temporaryRedirect(new URI(redirectUrl)).build();
    }

    @GET
    @Path("/check_auth")
    public Response checkUser(@CookieParam("wiki_session") String wikiSession) {
        if (wikiSession != null && accessTokens.getIfPresent(wikiSession) != null) {
            return Response.ok().build();
        } else {
            return status(FORBIDDEN).build();
        }
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
