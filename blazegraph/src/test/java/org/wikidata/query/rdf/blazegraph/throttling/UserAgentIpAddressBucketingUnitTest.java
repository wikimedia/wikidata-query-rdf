package org.wikidata.query.rdf.blazegraph.throttling;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.servlet.http.HttpServletRequest;

import org.junit.Before;
import org.junit.Test;

public class UserAgentIpAddressBucketingUnitTest {

    private UserAgentIpAddressBucketing bucketting;

    @Before
    public void createBuckettingUnderTest() {
        bucketting = new UserAgentIpAddressBucketing();
    }

    @Test
    public void sameUserAgentAndIpAddressAreInTheSameBucket() {
        Object bucket1 = bucketting.bucket(mockRequest("1.2.3.4", "UA1"));
        Object bucket2 = bucketting.bucket(mockRequest("1.2.3.4", "UA1"));

        assertThat(bucket1, equalTo(bucket2));
        assertThat(bucket1.hashCode(), equalTo(bucket2.hashCode()));
    }

    @Test
    public void nullUserAgentAndNonNullIpAddressAreInTheSameBucket() {
        Object bucket1 = bucketting.bucket(mockRequest("1.2.3.4", null));
        Object bucket2 = bucketting.bucket(mockRequest("1.2.3.4", null));

        assertThat(bucket1, equalTo(bucket2));
        assertThat(bucket1.hashCode(), equalTo(bucket2.hashCode()));
    }

    @Test
    public void nullUserAgentAndNullIpAddressAreInTheSameBucket() {
        Object bucket1 = bucketting.bucket(mockRequest(null, null));
        Object bucket2 = bucketting.bucket(mockRequest(null, null));

        assertThat(bucket1, equalTo(bucket2));
        assertThat(bucket1.hashCode(), equalTo(bucket2.hashCode()));
    }

    @Test
    public void differentUserAgentsAreInDifferentBuckets() {
        Object bucket1 = bucketting.bucket(mockRequest("1.2.3.4", "UA1"));
        Object bucket2 = bucketting.bucket(mockRequest("1.2.3.4", "UA2"));

        assertThat(bucket1, not(equalTo(bucket2)));
        assertThat(bucket1.hashCode(), not(equalTo(bucket2.hashCode())));
    }

    @Test
    public void differentIpAddressesAreInDifferentBuckets() {
        Object bucket1 = bucketting.bucket(mockRequest("1.2.3.4", "UA1"));
        Object bucket2 = bucketting.bucket(mockRequest("4.3.2.1", "UA1"));

        assertThat(bucket1, not(equalTo(bucket2)));
        assertThat(bucket1.hashCode(), not(equalTo(bucket2.hashCode())));
    }

    private HttpServletRequest mockRequest(String ipAddress, String userAgent) {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRemoteAddr()).thenReturn(ipAddress);
        when(request.getHeader("User-Agent")).thenReturn(userAgent);
        return request;
    }

}
