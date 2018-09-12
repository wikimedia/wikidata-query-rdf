package org.wikidata.query.rdf.blazegraph.throttling;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class RegexpBucketingTest {
    private RegexpBucketing bucketing;

    @Before
    public void createBuckettingUnderTest() {
        bucketing = new RegexpBucketing();
    }

    private HttpServletRequest mockRequest(String query) {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getParameter("query")).thenReturn(query);
        return request;
    }

    @Test
    public void testNoPatterns() {
        assertThat(bucketing.bucket(mockRequest("test 123")), nullValue());
    }

    @Test
    public void testPatterns() {
        bucketing.setPatterns(ImmutableList.of(
                Pattern.compile("matchme\\d+", Pattern.DOTALL),
                Pattern.compile("also [a-zA-Z]+ match", Pattern.DOTALL)
        ));
        assertThat(bucketing.bucket(mockRequest("test matchme123")), not(nullValue()));
        assertThat(bucketing.bucket(mockRequest("test matchme678 and some")),
                equalTo(bucketing.bucket(mockRequest("test matchme453"))));
        assertThat(bucketing.bucket(mockRequest("also THis matches")), not(nullValue()));
        assertThat(bucketing.bucket(mockRequest("but this does not")), nullValue());
    }
}
