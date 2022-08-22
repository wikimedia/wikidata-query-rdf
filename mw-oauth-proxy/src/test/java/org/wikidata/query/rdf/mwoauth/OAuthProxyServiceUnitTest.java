package org.wikidata.query.rdf.mwoauth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.wikidata.query.rdf.mwoauth.OAuthProxyService.OAUTH_COOKIE_NAME;
import static org.wikidata.query.rdf.mwoauth.OAuthProxyService.SESSION_COOKIE_NAME;
import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.Status.OK;
import static javax.ws.rs.core.Response.Status.TEMPORARY_REDIRECT;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import javax.servlet.ServletConfig;
import javax.ws.rs.core.Cookie;
import javax.ws.rs.core.NewCookie;
import javax.ws.rs.core.Response;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.wikidata.query.rdf.mwoauth.OAuthProxyService.PreAuthSessionState;

import com.github.scribejava.core.model.OAuth1AccessToken;
import com.github.scribejava.core.model.OAuth1RequestToken;
import com.github.scribejava.core.oauth.OAuth10aService;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

@RunWith(MockitoJUnitRunner.class)
@SuppressWarnings("checkstyle:classfanoutcomplexity")
public class OAuthProxyServiceUnitTest {

    private static final String AUTHORIZE_URL = "http://localhost/authorize/";
    private static final String AUTHENTICATE_URL = "http://localhost/authenticate/";
    private static final String OAUTH_TOKEN_STRING = "token";
    private static final String OAUTH_VERIFIER_STR = "oauth_verifying_string";
    public static final String WIKI_LOGOUT_LINK = "https://commons.wikimedia.org/w/index.php?title=Special:UserLogout";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private OAuthProxyService sut;
    private OAuth10aService mwoauthServiceMock;
    private OAuthIdentifyService identifyMock;

    @Before
    public void setUp() throws Exception {
        mwoauthServiceMock = getMockedMWOAuthService();
        identifyMock = mock(OAuthIdentifyService.class);
        sut = makeService(ImmutableList.of("banned", "also_banned"));
    }

    private OAuthProxyService makeService(Collection<String> bannedUsernames) throws Exception {
        OAuthProxyService service = new OAuthProxyService();
        service.init(
            getMockedConfig(ImmutableMap.of(
                OAuthProxyConfig.ACCESS_TOKEN_SECRET, "not_secret",
                OAuthProxyConfig.SESSION_STORE_KEY_PREFIX, "dummy:prefix",
                OAuthProxyConfig.WIKI_LOGOUT_LINK_PROPERTY, WIKI_LOGOUT_LINK,
                OAuthProxyConfig.BANNED_USERNAMES_PATH_PROPERTY, makeBannedFile(bannedUsernames)
            )),
            mwoauthServiceMock, identifyMock,
            getMockedSessionStore(1, PreAuthSessionState.class),
            getMockedSessionStore(1, OAuth1AccessToken.class));
        return service;
    }

    @Test
    public void shouldForbidNonLoggedUser() {
        Response response = sut.checkUser("invalid session");
        assertThat(response.getStatus()).isEqualTo(FORBIDDEN.getStatusCode());
    }

    @Test
    public void shouldAllowLoggedInUser() throws Exception {
        when(identifyMock.getUsername(any())).thenReturn(Optional.of("fake_username"));
        Response checkLoginResponse = sut.checkLogin(null, null);

        assertThat(checkLoginResponse.getStatus()).isEqualTo(TEMPORARY_REDIRECT.getStatusCode());
        assertThat(extractRedirectLocation(checkLoginResponse)).isEqualTo(new URI(AUTHENTICATE_URL));

        //at this time user would authenticate with MediaWiki...


        String redirectUrl = "http://localhost/redirect";
        Response verifyResponse = sut.oauthVerify(OAUTH_VERIFIER_STR, OAUTH_TOKEN_STRING, redirectUrl);
        NewCookie newCookie = verifyResponse.getCookies().get(SESSION_COOKIE_NAME);
        assertThat(newCookie.getMaxAge()).isEqualTo(2 * 60 * 60);
        String wikiSession = newCookie.getValue();

        assertThat(verifyResponse.getStatus()).isEqualTo(TEMPORARY_REDIRECT.getStatusCode());
        assertThat(extractRedirectLocation(verifyResponse)).isEqualTo(new URI(redirectUrl));

        Response checkUserResponse = sut.checkUser(wikiSession);
        assertThat(checkUserResponse.getStatus()).isEqualTo(OK.getStatusCode());
    }

    @Test
    public void shouldRememberReturnUrl() throws Exception {
        String url = "http://some.where/";
        when(identifyMock.getUsername(any())).thenReturn(Optional.of("fake_username"));
        sut.checkLogin(url, null);
        Response verifyResponse = sut.oauthVerify(OAUTH_VERIFIER_STR, OAUTH_TOKEN_STRING, null);
        assertThat(verifyResponse.getLocation()).hasToString(url);
    }

    @Test
    public void shouldReturnForbiddenIfTokenWasCleared() throws Exception {
        //1st user request for request token
        sut.checkLogin(null, null);
        // 2nd user request for request token. Token clearing is simulated here because our cache is
        // only allowed to hold a single value during test.
        OAuth1RequestToken requestToken = new OAuth1RequestToken("new token", "tokenSecret");
        when(mwoauthServiceMock.getRequestToken()).thenReturn(requestToken);
        when(mwoauthServiceMock.getAuthorizationUrl(requestToken)).thenReturn(AUTHORIZE_URL);
        sut.checkLogin(null, null);
        //1st user request for session verification
        Response verifyResponse = sut.oauthVerify(OAUTH_VERIFIER_STR, OAUTH_TOKEN_STRING, "http://localhost");
        assertThat(verifyResponse.getStatus()).isEqualTo(FORBIDDEN.getStatusCode());
    }

    @Test
    public void logoutShouldDeleteCookieAndRedirect() throws Exception {
        when(identifyMock.getUsername(any())).thenReturn(Optional.of("fake_username"));
        sut.checkLogin(null, null);
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
            .isEqualTo(new NewCookie(SESSION_COOKIE_NAME, "deleted", "/", null, Cookie.DEFAULT_VERSION,
            null, 0,  new Date(0), true, true));

        assertThat(logoutResponse.getCookies())
            .extractingByKey(OAUTH_COOKIE_NAME)
            .isEqualTo(new NewCookie(OAUTH_COOKIE_NAME, "deleted", "/", null, Cookie.DEFAULT_VERSION,
                null, 0, new Date(0), true, true));
    }

    @Test
    public void noSessionCookieForBannedUsers() throws Exception {
        when(identifyMock.getUsername(any())).thenReturn(Optional.of("banned"));
        Response checkLoginResponse = sut.checkLogin(null, null);

        assertThat(checkLoginResponse.getStatus()).isEqualTo(TEMPORARY_REDIRECT.getStatusCode());
        assertThat(extractRedirectLocation(checkLoginResponse)).isEqualTo(new URI(AUTHENTICATE_URL));

        //at this time user would authenticate with MediaWiki...


        String redirectUrl = "http://localhost/redirect";
        Response verifyResponse = sut.oauthVerify(OAUTH_VERIFIER_STR, OAUTH_TOKEN_STRING, redirectUrl);

        assertThat(verifyResponse.getStatus()).isEqualTo(FORBIDDEN.getStatusCode());
        assertThat(verifyResponse.getCookies().get(SESSION_COOKIE_NAME)).isNull();
    }

    @Test
    public void validTokensWithBannedUsernamesAreRejected() throws Exception {
        when(identifyMock.getUsername(any())).thenReturn(Optional.of("not_banned_yet"));
        Response checkLoginResponse = sut.checkLogin(null, null);

        assertThat(checkLoginResponse.getStatus()).isEqualTo(TEMPORARY_REDIRECT.getStatusCode());
        assertThat(extractRedirectLocation(checkLoginResponse)).isEqualTo(new URI(AUTHENTICATE_URL));

        //at this time user would authenticate with MediaWiki...

        String redirectUrl = "http://localhost/redirect";
        Response verifyResponse = sut.oauthVerify(OAUTH_VERIFIER_STR, OAUTH_TOKEN_STRING, redirectUrl);
        assertThat(verifyResponse.getStatus()).isEqualTo(TEMPORARY_REDIRECT.getStatusCode());
        String wikiSession = verifyResponse.getCookies().get(SESSION_COOKIE_NAME).getValue();

        // A valid token is issued for that user
        Response checkUserResponse = sut.checkUser(wikiSession);
        assertThat(checkUserResponse.getStatus()).isEqualTo(OK.getStatusCode());

        // When we restart the instance with a new ban list including the username
        // that token will no longer validate.
        OAuthProxyService serviceIncludingBan = makeService(ImmutableList.of("also_banned", "not_banned_yet"));
        Response checkBannedResponse = serviceIncludingBan.checkUser(wikiSession);
        assertThat(checkBannedResponse.getStatus()).isEqualTo(FORBIDDEN.getStatusCode());
    }

    @Test
    public void shouldReauthFromAccessTokenIfAvailable() throws Exception {
        when(identifyMock.getUsername(any())).thenReturn(Optional.of("any_user"));
        // Attempt login, get bounced over to the external oauth provider
        Response checkLoginResponse = sut.checkLogin(null, null);
        assertThat(checkLoginResponse.getStatus()).isEqualTo(TEMPORARY_REDIRECT.getStatusCode());
        // User returned and becomes authorized
        String redirectUrl = "http://localhost/redirect";
        Response verifyResponse = sut.oauthVerify(OAUTH_VERIFIER_STR, OAUTH_TOKEN_STRING, redirectUrl);
        assertThat(verifyResponse.getStatus()).isEqualTo(TEMPORARY_REDIRECT.getStatusCode());
        String wikiSession = verifyResponse.getCookies().get(SESSION_COOKIE_NAME).getValue();
        String oauthToken = verifyResponse.getCookies().get(OAUTH_COOKIE_NAME).getValue();

        // When we arrive at /check_login with the oauthToken the session is refreshed and the user
        // is forwarded to their destination.
        Response secondCheckLoginResponse = sut.checkLogin(redirectUrl, oauthToken);
        assertThat(secondCheckLoginResponse.getCookies()).containsKey(SESSION_COOKIE_NAME);
        assertThat(secondCheckLoginResponse.getStatus()).isEqualTo(TEMPORARY_REDIRECT.getStatusCode());
        assertThat(secondCheckLoginResponse.getLocation()).hasToString(redirectUrl);
    }

    @Test
    public void shouldFailReauthIfAccessTokenCantFetchUsername() throws Exception {
        when(identifyMock.getUsername(any())).thenReturn(Optional.of("any_user"));
        // Attempt login, get bounced over to the external oauth provider
        Response checkLoginResponse = sut.checkLogin(null, null);
        assertThat(checkLoginResponse.getStatus()).isEqualTo(TEMPORARY_REDIRECT.getStatusCode());
        // User returned and becomes authorized
        String redirectUrl = "http://localhost/redirect";
        Response verifyResponse = sut.oauthVerify(OAUTH_VERIFIER_STR, OAUTH_TOKEN_STRING, redirectUrl);
        assertThat(verifyResponse.getStatus()).isEqualTo(TEMPORARY_REDIRECT.getStatusCode());
        String wikiSession = verifyResponse.getCookies().get(SESSION_COOKIE_NAME).getValue();
        String oauthToken = verifyResponse.getCookies().get(OAUTH_COOKIE_NAME).getValue();

        // When we arrive at /check_login with an oauthToken that doesn't resolve into a username
        // we should not issue a session cookie and the user should be forwarded to the oauth provider
        when(identifyMock.getUsername(any())).thenReturn(Optional.empty());
        Response secondCheckLoginResponse = sut.checkLogin(redirectUrl, oauthToken);
        assertThat(secondCheckLoginResponse.getCookies()).doesNotContainKey(SESSION_COOKIE_NAME);
        assertThat(secondCheckLoginResponse.getStatus()).isEqualTo(TEMPORARY_REDIRECT.getStatusCode());
        assertThat(secondCheckLoginResponse.getLocation()).hasToString(AUTHENTICATE_URL);
    }

    @Test
    public void roundTripPreAuthSessionState() throws Exception {
        roundTripSerde(
            OAuthProxyService.preAuthSessionStateSerde("example"),
            new PreAuthSessionState(
                new OAuth1RequestToken("some", "value"),
                Optional.empty()
            )
        );

        roundTripSerde(
            OAuthProxyService.preAuthSessionStateSerde("example"),
            new PreAuthSessionState(
                new OAuth1RequestToken("other", "content"),
                Optional.of(new URI("https://wikimedia.org"))
            )
        );
    }

    @Test
    public void roundTripOAuth1AccessToken() throws Exception {
        roundTripSerde(
            OAuthProxyService.oAuth1AccessTokenSerde("example"),
            new OAuth1AccessToken("token", "tokenSecret")
        );
    }

    private <T> void roundTripSerde(KaskSessionStore.Serde<T> serde, T value) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            serde.valueEncoder(oos, value);
        }
        T decoded = serde.valueDecoder(new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray())));
        assertThat(decoded).isEqualTo(value);
    }

    private URI extractRedirectLocation(Response verifyResponse) {
        return (URI) verifyResponse.getHeaders().get("location").get(0);
    }

    private OAuthProxyConfig getMockedConfig(Map<String, String> values) {
        ServletConfig servletConfig = mock(ServletConfig.class);
        values.forEach((k, v) -> when(servletConfig.getInitParameter(k)).thenReturn(v));
        return new OAuthProxyConfig(servletConfig);
    }

    private String makeBannedFile(Collection<String> bannedUsernames) throws IOException {
        File file = folder.newFile();
        try (PrintWriter out = new PrintWriter(Files.newBufferedWriter(file.toPath()))) {
            bannedUsernames.forEach(out::println);
        }
        return file.getAbsolutePath();
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

    private <T> KaskSessionStore<T> getMockedSessionStore(long maximumSize, Class<T> clazz) throws IOException {
        Cache<String, T> cache = CacheBuilder.newBuilder().maximumSize(maximumSize).build();
        KaskSessionStore<T> sessionStore = mock(KaskSessionStore.class);

        when(sessionStore.getIfPresent(anyString())).then((args) ->
                cache.getIfPresent(args.getArgumentAt(0, String.class)));

        doAnswer((args) -> {
            cache.put(args.getArgumentAt(0, String.class), args.getArgumentAt(1, clazz));
            return null;
        }).when(sessionStore).put(anyString(), isA(clazz));

        doAnswer((args) -> {
            cache.invalidate(args.getArgumentAt(0, String.class));
            return null;
        }).when(sessionStore).invalidate(anyString());

        return sessionStore;
    }
}
