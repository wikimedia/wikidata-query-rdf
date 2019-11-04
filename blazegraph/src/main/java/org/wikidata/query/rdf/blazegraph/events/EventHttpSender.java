package org.wikidata.query.rdf.blazegraph.events;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;

import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.blazegraph.JacksonUtil;

import com.fasterxml.jackson.databind.ObjectWriter;

public class EventHttpSender implements EventSender, AutoCloseable {
    public static final int DEFAULT_CON_TIMEOUT = 5;
    public static final int DEFAULT_READ_TIMEOUT = 5;

    private final CloseableHttpClient httpClient;
    private final String eventGateUri;
    private final ObjectWriter objectWriter;
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    public EventHttpSender(CloseableHttpClient httpClient, String eventGateUri, ObjectWriter objectWriter) {
        this.httpClient = httpClient;
        this.eventGateUri = eventGateUri;
        this.objectWriter = objectWriter;
    }

    public static EventHttpSender build(String eventGateUri, int timeOut, int connectionTimeout) {
        RequestConfig config = RequestConfig.custom().setConnectTimeout(connectionTimeout)
                .setSocketTimeout(timeOut)
                .build();
        CloseableHttpClient httpclient = HttpClients.custom()
                .setDefaultRequestConfig(config)
                .build();
        return new EventHttpSender(httpclient, eventGateUri, JacksonUtil.DEFAULT_OBJECT_WRITER);
    }

    public boolean push(Event event) {
        return push(Collections.singletonList(event)) > 0;
    }

    public int push(Collection<Event> events) {
        HttpPost post = new HttpPost(eventGateUri);
        try {
            post.setEntity(httpEntity(events));
            try (CloseableHttpResponse response = httpClient.execute(post)) {
                if (response.getStatusLine().getStatusCode() >= 400) {
                    log.error("Cannot send events to eventgate endpoint: {}: {}", eventGateUri, response.getStatusLine());
                    return 0;
                }
            }
            return events.size();
        } catch (IOException e) {
            log.error("Cannot send events to eventgate endpoint: {}", eventGateUri, e);
            return 0;
        }
    }

    @Override
    public void close() {
        try {
            httpClient.close();
        } catch (IOException e) {
            log.error("Cannot close the http client", e);
        }
    }

    private HttpEntity httpEntity(Collection<Event> events) {
        AbstractHttpEntity entity = new AbstractHttpEntity() {
            @Override
            public boolean isRepeatable() {
                return true;
            }

            @Override
            public long getContentLength() {
                return -1L;
            }

            @Override
            public InputStream getContent() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void writeTo(OutputStream outputStream) throws IOException {
                objectWriter.writeValue(outputStream, events);
            }

            @Override
            public boolean isStreaming() {
                return false;
            }
        };
        entity.setContentType(ContentType.APPLICATION_JSON.toString());
        return entity;
    }
}
