package org.wikidata.query.rdf.blazegraph.throttling;

import org.junit.Before;
import org.junit.Test;
import org.wikidata.query.rdf.blazegraph.throttling.UserAgentIpAddressBucketing.Bucket;

import javax.servlet.http.HttpServletRequest;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UserAgentIpAddressBucketingTest {

    private UserAgentIpAddressBucketing bucketting;

    @Before
    public void createBuckettingUnderTest() {
        bucketting = new UserAgentIpAddressBucketing();
    }

    @Test
    public void sameUserAgentAndIpAddressAreInTheSameBucket() {
        Bucket bucket1 = bucketting.bucket(mockRequest("1.2.3.4", "UA1"));
        Bucket bucket2 = bucketting.bucket(mockRequest("1.2.3.4", "UA1"));

        assertThat(bucket1, equalTo(bucket2));
        assertThat(bucket1.hashCode(), equalTo(bucket2.hashCode()));
    }

    @Test
    public void nullUserAgentAndNonNullIpAddressAreInTheSameBucket() {
        Bucket bucket1 = bucketting.bucket(mockRequest("1.2.3.4", null));
        Bucket bucket2 = bucketting.bucket(mockRequest("1.2.3.4", null));

        assertThat(bucket1, equalTo(bucket2));
        assertThat(bucket1.hashCode(), equalTo(bucket2.hashCode()));
    }

    @Test
    public void nullUserAgentAndNullIpAddressAreInTheSameBucket() {
        Bucket bucket1 = bucketting.bucket(mockRequest(null, null));
        Bucket bucket2 = bucketting.bucket(mockRequest(null, null));

        assertThat(bucket1, equalTo(bucket2));
        assertThat(bucket1.hashCode(), equalTo(bucket2.hashCode()));
    }

    @Test
    public void differentUserAgentsAreInDifferentBuckets() {
        Bucket bucket1 = bucketting.bucket(mockRequest("1.2.3.4", "UA1"));
        Bucket bucket2 = bucketting.bucket(mockRequest("1.2.3.4", "UA2"));

        assertThat(bucket1, not(equalTo(bucket2)));
        assertThat(bucket1.hashCode(), not(equalTo(bucket2.hashCode())));
    }

    @Test
    public void differentIpAddressesAreInDifferentBuckets() {
        Bucket bucket1 = bucketting.bucket(mockRequest("1.2.3.4", "UA1"));
        Bucket bucket2 = bucketting.bucket(mockRequest("4.3.2.1", "UA1"));

        assertThat(bucket1, not(equalTo(bucket2)));
        assertThat(bucket1.hashCode(), not(equalTo(bucket2.hashCode())));
    }

    private HttpServletRequest mockRequest(String ipAddress, String userAgent) {
        HttpServletRequest request1 = mock(HttpServletRequest.class);
        when(request1.getRemoteAddr()).thenReturn(ipAddress);
        when(request1.getHeader("User-Agent")).thenReturn(userAgent);
        return request1;
    }

}
