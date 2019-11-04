package org.wikidata.query.rdf.blazegraph.events;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.http.HttpVersion;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicStatusLine;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.wikidata.query.rdf.blazegraph.JacksonUtil;

public class EventHttpSenderUnitTest {
    @Test
    public void testSendOneEvent() throws IOException {
        String eventGateHost = "https://eventgate.test.local/v1/events";
        CloseableHttpClient client = mock(CloseableHttpClient.class);
        CloseableHttpResponse resp = mock(CloseableHttpResponse.class);

        when(resp.getStatusLine()).thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, 200, "OK"));
        when(client.execute(any(HttpUriRequest.class))).thenReturn(resp);

        Event event = EventTestUtils.newTestEvent();
        EventHttpSender sender = new EventHttpSender(client, eventGateHost, JacksonUtil.DEFAULT_OBJECT_WRITER);
        sender.push(event);

        ArgumentCaptor<HttpUriRequest> captor = ArgumentCaptor.forClass(HttpUriRequest.class);
        verify(client, times(1)).execute(captor.capture());
        verify(resp, times(1)).close();
        HttpUriRequest request = captor.getValue();

        assertThat(request.getMethod()).isEqualTo("POST");
        assertThat(request.getURI().toString()).isEqualTo(eventGateHost);
        assertThat(request).isInstanceOf(HttpPost.class);
        HttpPost post = (HttpPost) request;
        assertThat(post.getEntity().getContentType().getValue()).isEqualTo(ContentType.APPLICATION_JSON.toString());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        post.getEntity().writeTo(baos);
        assertThat(new String(baos.toByteArray(), StandardCharsets.UTF_8)).isEqualTo("[" + EventTestUtils.testEventJsonString() + "]");
    }

    @Test
    public void testSendMultipleEvents() throws IOException {
        String eventGateHost = "https://eventgate.test.local/v1/events";
        CloseableHttpClient client = mock(CloseableHttpClient.class);
        CloseableHttpResponse resp = mock(CloseableHttpResponse.class);

        when(resp.getStatusLine()).thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, 200, "OK"));
        when(client.execute(any(HttpUriRequest.class))).thenReturn(resp);

        int n = 0;
        Collection<Event> events = Stream.generate(EventTestUtils::newTestEvent)
                .limit(n)
                .collect(Collectors.toList());

        String expectedEventsAsJson = Stream.generate(EventTestUtils::testEventJsonString)
                .limit(n)
                .collect(Collectors.joining(",", "[",  "]"));

        EventHttpSender sender = new EventHttpSender(client, eventGateHost, JacksonUtil.DEFAULT_OBJECT_WRITER);
        assertThat(sender.push(events)).isEqualTo(events.size());

        ArgumentCaptor<HttpUriRequest> captor = ArgumentCaptor.forClass(HttpUriRequest.class);
        verify(client, times(1)).execute(captor.capture());
        verify(resp, times(1)).close();
        HttpUriRequest request = captor.getValue();

        assertThat(request.getMethod()).isEqualTo("POST");
        assertThat(request.getURI().toString()).isEqualTo(eventGateHost);
        assertThat(request).isInstanceOf(HttpPost.class);
        HttpPost post = (HttpPost) request;
        assertThat(post.getEntity().getContentType().getValue()).isEqualTo(ContentType.APPLICATION_JSON.toString());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        post.getEntity().writeTo(baos);

        assertThat(new String(baos.toByteArray(), StandardCharsets.UTF_8)).isEqualTo(expectedEventsAsJson);
    }

    @Test
    public void testDoesNotBlowUpOnFailure() throws IOException {
        String eventGateHost = "https://eventgate.test.local/v1/events";
        CloseableHttpClient client = mock(CloseableHttpClient.class);
        when(client.execute(any(HttpUriRequest.class))).thenThrow(new IOException("failure"));
        EventHttpSender sender = new EventHttpSender(client, eventGateHost, JacksonUtil.DEFAULT_OBJECT_WRITER);
        assertThat(sender.push(EventTestUtils.newTestEvent())).isFalse();
        assertThat(sender.push(Collections.singleton(EventTestUtils.newTestEvent()))).isEqualTo(0);
        // we should not fail here
    }

    @Test
    public void testPushReturnsFalseOnHttpErrorCode() throws IOException {
        String eventGateHost = "https://eventgate.test.local/v1/events";
        CloseableHttpClient client = mock(CloseableHttpClient.class);
        CloseableHttpResponse resp = mock(CloseableHttpResponse.class);

        when(resp.getStatusLine()).thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, 404, "Not found"));
        when(client.execute(any(HttpUriRequest.class))).thenReturn(resp);

        EventHttpSender sender = new EventHttpSender(client, eventGateHost, JacksonUtil.DEFAULT_OBJECT_WRITER);
        assertThat(sender.push(EventTestUtils.newTestEvent())).isFalse();
        assertThat(sender.push(Collections.singleton(EventTestUtils.newTestEvent()))).isEqualTo(0);

        verify(resp, times(2)).close();
    }

    @Test
    @SuppressWarnings("EmptyBlock")
    public void testClientIsClosed() throws Exception {
        String eventGateHost = "https://eventgate.test.local/v1/events";
        CloseableHttpClient client = mock(CloseableHttpClient.class);
        try (EventHttpSender sender = new EventHttpSender(client, eventGateHost, JacksonUtil.DEFAULT_OBJECT_WRITER)) {
        }
        verify(client, times(1)).close();
    }
}
