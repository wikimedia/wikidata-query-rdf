package org.wikidata.query.rdf.blazegraph.throttling;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class RegexpBucketingUnitTest {

    public static final ImmutableList<Pattern> TEST_PATTERNS = ImmutableList.of(
            Pattern.compile("matchme\\d+", Pattern.DOTALL),
            Pattern.compile("also [a-zA-Z]+ match", Pattern.DOTALL)
    );

    @Test
    public void nullBucketWhenNoPatterns() {
        Bucketing bucketing = new RegexpBucketing(ImmutableList.of());

        assertThat(bucketing.bucket(mockRequest("test 123"))).isNull();
    }

    @Test
    public void nullBucketWhenNoMatchingPattern() {
        Bucketing bucketing = new RegexpBucketing(TEST_PATTERNS);

        assertThat(bucketing.bucket(mockRequest("but this does not"))).isNull();
    }

    @Test
    public void nonNullBucketWhenMatchingPattern() {
        Bucketing bucketing = new RegexpBucketing(TEST_PATTERNS);

        assertThat(bucketing.bucket(mockRequest("test matchme123"))).isNotNull();
        assertThat(bucketing.bucket(mockRequest("also THis matches"))).isNotNull();
    }

    @Test
    public void queriesMatchingSamePatternAreInSameBucket() {
        Bucketing bucketing = new RegexpBucketing(TEST_PATTERNS);

        Object bucket1 = bucketing.bucket(mockRequest("test matchme678 and some"));
        Object bucket2 = bucketing.bucket(mockRequest("test matchme453"));

        assertThat(bucket1).isEqualTo(bucket2);
    }

    private HttpServletRequest mockRequest(String query) {
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getParameter("query")).thenReturn(query);
        return request;
    }

}
