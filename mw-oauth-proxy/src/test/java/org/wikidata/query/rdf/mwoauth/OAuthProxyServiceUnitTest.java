package org.wikidata.query.rdf.mwoauth;


import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.Status.OK;
import static javax.ws.rs.core.Response.Status.TEMPORARY_REDIRECT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.wikidata.query.rdf.mwoauth.OAuthProxyService.SESSION_COOKIE_NAME;


import java.io.IOException;
import java.net.URI;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.servlet.ServletConfig;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.NewCookie;
import javax.ws.rs.core.Response;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.github.scribejava.core.model.OAuth1AccessToken;
import com.github.scribejava.core.model.OAuth1RequestToken;
import com.github.scribejava.core.oauth.OAuth10aService;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;

@RunWith(MockitoJUnitRunner.class)
public class OAuthProxyServiceUnitTest {

    private static final String AUTHORIZE_URL = "http://localhost/authorize/";
    private static final String AUTHENTICATE_URL = "http://localhost/authenticate/";
    private static final String OAUTH_TOKEN_STRING = "token";
    private static final String OAUTH_VERIFIER_STR = "oauth_verifying_string";
    public static final String WIKI_LOGOUT_LINK = "https://commons.wikimedia.org/w/index.php?title=Special:UserLogout";


    private OAuthProxyService sut;
    private OAuth10aService mwoauthServiceMock;

    @Before
    public void setUp() throws Exception {
        sut = new OAuthProxyService();
        mwoauthServiceMock = getMockedMWOAuthService();
        sut = new OAuthProxyService();
        sut.init(getMockedConfig(ImmutableMap.of(
            OAuthProxyConfig.ACCESS_TOKEN_SECRET, "not_secret",
            OAuthProxyConfig.SESSION_STORE_KEY_PREFIX, "dummy:prefix",
            OAuthProxyConfig.WIKI_LOGOUT_LINK_PROPERTY, WIKI_LOGOUT_LINK
        )), mwoauthServiceMock, getMockedSessionStore(1));
    }

    @Test
    public void shouldForbidNonLoggedUser() {
        Response response = sut.checkUser("invalid session");
        assertThat(response.getStatus()).isEqualTo(FORBIDDEN.getStatusCode());
    }

    @Test
    public void shouldAllowLoggedInUser() throws Exception {
        Response checkLoginResponse = sut.checkLogin();

        assertThat(checkLoginResponse.getStatus()).isEqualTo(TEMPORARY_REDIRECT.getStatusCode());
        assertThat(extractRedirectLocation(checkLoginResponse)).isEqualTo(new URI(AUTHENTICATE_URL));

        //at this time user would authenticate with MediaWiki...


        String redirectUrl = "http://localhost/redirect";
        Response verifyResponse = sut.oauthVerify(OAUTH_VERIFIER_STR, OAUTH_TOKEN_STRING, redirectUrl);
        String wikiSession = verifyResponse.getCookies().get(SESSION_COOKIE_NAME).getValue();

        assertThat(verifyResponse.getStatus()).isEqualTo(TEMPORARY_REDIRECT.getStatusCode());
        assertThat(extractRedirectLocation(verifyResponse)).isEqualTo(new URI(redirectUrl));

        Response checkUserResponse = sut.checkUser(wikiSession);
        assertThat(checkUserResponse.getStatus()).isEqualTo(OK.getStatusCode());
    }

    @Test
    public void shouldReturnForbiddenIfTokenWasCleared() throws Exception {
        //1st user request for request token
        sut.checkLogin();
        // 2nd user request for request token. Token clearing is simulated here because our cache is
        // only allowed to hold a single value during test.
        OAuth1RequestToken requestToken = new OAuth1RequestToken("new token", "tokenSecret");
        when(mwoauthServiceMock.getRequestToken()).thenReturn(requestToken);
        when(mwoauthServiceMock.getAuthorizationUrl(requestToken)).thenReturn(AUTHORIZE_URL);
        sut.checkLogin();
        //1st user request for session verification
        Response verifyResponse = sut.oauthVerify(OAUTH_VERIFIER_STR, OAUTH_TOKEN_STRING, "http://localhost");
        assertThat(verifyResponse.getStatus()).isEqualTo(FORBIDDEN.getStatusCode());
    }

    @Test
    public void logoutShouldDeleteCookieAndRedirect() throws Exception {
        sut.checkLogin();
        String redirectUrl = "http://localhost/redirect";
        Response verifyResponse = sut.oauthVerify(OAUTH_VERIFIER_STR, OAUTH_TOKEN_STRING, redirectUrl);
        String wikiSession = verifyResponse.getCookies().get(SESSION_COOKIE_NAME).getValue();


        Response logoutResponse = sut.logout(wikiSession);

        // Expected limitation, access tokens are irrevocable.
        assertThat(sut.checkUser(wikiSession).getStatus()).isEqualTo(OK.getStatusCode());

        assertThat(logoutResponse.getStatus()).isEqualTo(TEMPORARY_REDIRECT.getStatusCode());
        assertThat(extractRedirectLocation(logoutResponse))
                .isEqualTo(new URI(WIKI_LOGOUT_LINK));

        assertThat(logoutResponse.getCookies())
                .extractingByKey(SESSION_COOKIE_NAME)
                .isEqualTo(new NewCookie(new Cookie(SESSION_COOKIE_NAME, "deleted"),
                        "", 0, new Date(0), true, true));
    }

    private URI extractRedirectLocation(Response verifyResponse) {
        return (URI) verifyResponse.getHeaders().get("location").get(0);
    }

    private OAuthProxyConfig getMockedConfig(Map<String, String> values) {
        ServletConfig servletConfig = mock(ServletConfig.class);
        values.forEach((k, v) -> when(servletConfig.getInitParameter(k)).thenReturn(v));
        return new OAuthProxyConfig(servletConfig);
    }

    private KaskSessionStore<OAuth1RequestToken> getMockedSessionStore() throws IOException {
        Cache<String, OAuth1RequestToken> cache = CacheBuilder.newBuilder().build();
        KaskSessionStore<OAuth1RequestToken> sessionStore = mock(KaskSessionStore.class);

        when(sessionStore.getIfPresent(anyString())).then((args) ->
            cache.getIfPresent(args.getArgumentAt(0, String.class)));

        doAnswer((args) -> {
            cache.put(args.getArgumentAt(0, String.class), args.getArgumentAt(1, OAuth1RequestToken.class));
            return null;
        }).when(sessionStore).put(anyString(), isA(OAuth1RequestToken.class));

        doAnswer((args) -> {
            cache.invalidate(args.getArgumentAt(0, String.class));
            return null;
        }).when(sessionStore).invalidate(anyString());

        return sessionStore;
    }

    private OAuth10aService getMockedMWOAuthService() throws IOException, InterruptedException, ExecutionException {
        OAuth10aService mwoauthServiceMock = mock(OAuth10aService.class);

        OAuth1RequestToken requestToken = new OAuth1RequestToken(OAUTH_TOKEN_STRING, "tokenSecret");
        when(mwoauthServiceMock.getRequestToken()).thenReturn(requestToken);
        when(mwoauthServiceMock.getAuthorizationUrl(requestToken)).thenReturn(AUTHORIZE_URL);
        OAuth1AccessToken accessToken = new OAuth1AccessToken("access_token", "access_token_secret");
        when(mwoauthServiceMock.getAccessToken(requestToken, OAUTH_VERIFIER_STR)).thenReturn(accessToken);

        return mwoauthServiceMock;
    }

    private KaskSessionStore<OAuth1RequestToken> getMockedSessionStore(long maximumSize) throws IOException {
        Cache<String, OAuth1RequestToken> cache = CacheBuilder.newBuilder().maximumSize(maximumSize).build();
        KaskSessionStore<OAuth1RequestToken> sessionStore = mock(KaskSessionStore.class);

        when(sessionStore.getIfPresent(anyString())).then((args) ->
            cache.getIfPresent(args.getArgumentAt(0, String.class)));

        doAnswer((args) -> {
            cache.put(args.getArgumentAt(0, String.class), args.getArgumentAt(1, OAuth1RequestToken.class));
            return null;
        }).when(sessionStore).put(anyString(), isA(OAuth1RequestToken.class));

        doAnswer((args) -> {
            cache.invalidate(args.getArgumentAt(0, String.class));
            return null;
        }).when(sessionStore).invalidate(anyString());

        return sessionStore;
    }
}
