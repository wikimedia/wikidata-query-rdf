package org.wikidata.query.rdf.mwoauth;


import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.Status.OK;
import static javax.ws.rs.core.Response.Status.TEMPORARY_REDIRECT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutionException;

import javax.ws.rs.core.Response;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.github.scribejava.core.model.OAuth1AccessToken;
import com.github.scribejava.core.model.OAuth1RequestToken;
import com.github.scribejava.core.oauth.OAuth10aService;

@RunWith(MockitoJUnitRunner.class)
public class OAuthProxyServiceUnitTest {

    private static final String AUTHORIZE_URL = "http://localhost/authorize/";
    private static final String AUTHENTICATE_URL = "http://localhost/authenticate/";
    private static final String OAUTH_TOKEN_STRING = "token";
    private static final String OAUTH_VERIFIER_STR = "oauth_verifying_string";

    private OAuthProxyService sut;
    private OAuth10aService mwoauthServiceMock;

    @Before
    public void setUp() throws Exception {
        mwoauthServiceMock = getMockedMWOAuthService();
        sut = new OAuthProxyService(mwoauthServiceMock, 1);
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

        String wikiSession = "wiki_session";
        String redirectUrl = "http://localhost/redirect";
        Response verifyResponse = sut.oauthVerify(OAUTH_VERIFIER_STR, OAUTH_TOKEN_STRING, wikiSession, redirectUrl);

        assertThat(verifyResponse.getStatus()).isEqualTo(TEMPORARY_REDIRECT.getStatusCode());
        assertThat(extractRedirectLocation(verifyResponse)).isEqualTo(new URI(redirectUrl));

        Response checkUserResponse = sut.checkUser(wikiSession);
        assertThat(checkUserResponse.getStatus()).isEqualTo(OK.getStatusCode());
    }

    @Test
    public void shouldReturnForbiddenIfTokenWasCleared() throws Exception {
        //1st user request for request token
        sut.checkLogin();
        //2nd user request for request token
        OAuth1RequestToken requestToken = new OAuth1RequestToken("new token", "tokenSecret");
        when(mwoauthServiceMock.getRequestToken()).thenReturn(requestToken);
        when(mwoauthServiceMock.getAuthorizationUrl(requestToken)).thenReturn(AUTHORIZE_URL);
        sut.checkLogin();
        //1st user request for session verification
        Response verifyResponse = sut.oauthVerify(OAUTH_VERIFIER_STR, OAUTH_TOKEN_STRING, "wikiSession", "http://localhost");
        assertThat(verifyResponse.getStatus()).isEqualTo(FORBIDDEN.getStatusCode());
    }

    private URI extractRedirectLocation(Response verifyResponse) {
        return (URI) verifyResponse.getHeaders().get("location").get(0);
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


}
