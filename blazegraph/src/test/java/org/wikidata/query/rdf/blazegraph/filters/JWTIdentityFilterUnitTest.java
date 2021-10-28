package org.wikidata.query.rdf.blazegraph.filters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;

import java.util.Map;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.google.common.collect.ImmutableMap;

@RunWith(MockitoJUnitRunner.class)
public class JWTIdentityFilterUnitTest {
    private Filter filter = new JWTIdentityFilter();

    private Map<String, String> hashConfig = ImmutableMap.of(
        "jwt-identity-cookie-name", "the_cookie",
        "jwt-identity-claim", "identity",
        "jwt-identity-secret", "not_secret"
    );

    @Mock private FilterConfig filterConfig;
    @Mock private FilterChain chain;
    @Captor private ArgumentCaptor<ServletRequest> filteredRequest;

    @Before
    public void setup() throws Exception {
        Mockito.when(filterConfig.getInitParameter(any()))
            .then(args -> hashConfig.get(args.getArgumentAt(0, String.class)));
        filter.init(filterConfig);
    }

    @Test
    public void withNoCookies() throws Exception {
        HttpServletRequest request = new MockHttpServletRequest();
        HttpServletRequest filteredRequest = doFilter(request);
        assertThat(filteredRequest.getRemoteUser()).isNull();
    }

    @Test
    public void withAnInvalidCookie() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setCookies(new Cookie("the_cookie", "let me in"));
        HttpServletRequest filteredRequest = doFilter(request);
        assertThat(filteredRequest.getRemoteUser()).isNull();
    }

    @Test
    public void withUnrelatedCookies() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setCookies(new Cookie("another_cookie", "let me in"));
        HttpServletRequest filteredRequest = doFilter(request);
        assertThat(filteredRequest.getRemoteUser()).isNull();
    }

    @Test
    public void withAValidCookie() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setCookies(new Cookie("the_cookie", validToken("the_user")));
        HttpServletRequest filteredRequest = doFilter(request);
        assertThat(filteredRequest.getRemoteUser()).isEqualTo("the_user");
    }

    @Test
    public void withVariousCookies() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.setCookies(
            new Cookie("another_cookie", "let me in"),
            new Cookie("the_cookie", validToken("the_user")));
        HttpServletRequest filteredRequest = doFilter(request);
        assertThat(filteredRequest.getRemoteUser()).isEqualTo("the_user");
    }

    private HttpServletRequest doFilter(HttpServletRequest request) throws Exception {
        MockHttpServletResponse response = new MockHttpServletResponse();
        filter.doFilter(request, response, chain);
        verify(chain).doFilter(filteredRequest.capture(), any(ServletResponse.class));
        return (HttpServletRequest)filteredRequest.getValue();
    }

    private String validToken(String identity) {
        return JWT.create().withClaim("identity", identity).sign(Algorithm.HMAC256("not_secret"));
    }
}
