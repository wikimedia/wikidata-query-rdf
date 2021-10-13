package org.wikidata.query.rdf.mwoauth;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.AbstractResponseHandler;
import org.apache.http.util.EntityUtils;

import lombok.SneakyThrows;

/**
 * Minimal client implementation for Kask
 *
 * Provides a bare-bones implementation of storing and requesting values from Kask. Error handling is minimal,
 * exceptions are thrown based on http codes which should be sufficient for the use case. While kask provides
 * error details, those are discarded here.
 *
 * See also: https://www.mediawiki.org/wiki/Kask
 */
public class KaskSessionStore<T> {
    private static final String BASE_URI = "/sessions/v1/";
    private final HttpClient httpClient;
    private final HttpHost host;
    private final Serde<T> serde;
    private final ResponseHandler<T> responseHandler;

    interface Serde<T> {
        String keyEncoder(String key);
        void valueEncoder(DataOutput os, T value) throws IOException;
        T valueDecoder(DataInput is) throws IOException;
    }

    KaskSessionStore(HttpClient httpClient, HttpHost host, Serde<T> serde) {
        this.httpClient = httpClient;
        this.host = host;
        this.serde = serde;
        responseHandler = new AbstractResponseHandler<T>() {
            @Override
            public T handleEntity(HttpEntity entity) throws IOException {
                if (entity.getContentLength() == 0) {
                    // Non-content requests, like a 200 OK response to PUT
                    return null;
                }
                T decoded = serde.valueDecoder(new ObjectInputStream(entity.getContent()));
                EntityUtils.consume(entity);
                return decoded;
            }
        };
    }

    public T getIfPresent(String key) throws IOException {
        HttpGet req = new HttpGet(path(key));
        req.setHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_OCTET_STREAM.getMimeType());
        try {
            return execute(req);
        } catch (HttpResponseException e) {
            if (e.getStatusCode() == 404) {
                return null;
            }
            throw e;
        }
    }

    private HttpEntity asEntity(T value) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            serde.valueEncoder(oos, value);
        }
        return new ByteArrayEntity(
            baos.toByteArray(), ContentType.APPLICATION_OCTET_STREAM);
    }

    public void put(String key, T value) throws IOException {
        HttpPost req = new HttpPost(path(key));
        req.setEntity(asEntity(value));
        execute(req);
    }

    public void invalidate(String key) throws IOException {
        execute(new HttpDelete(path(key)));
    }

    @SneakyThrows
    private String path(String key) {
        // It's not clearly preferable for the encoder to have the requirement of
        // returning a valid uri, since we only ask for a string.
        return BASE_URI + URLEncoder.encode(serde.keyEncoder(key), StandardCharsets.UTF_8.name());
    }

    private T execute(HttpRequest target) throws IOException {
        return httpClient.execute(host, target, responseHandler);
    }
}
