package org.wikidata.query.rdf.mwoauth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KaskSessionStoreUnitTest {
    private static final HttpHost httpHost = new HttpHost("dummyhost", 1234, "https");
    private HttpClient httpClient;
    private HttpResponse httpResponse;
    private StatusLine statusLine;
    private HttpEntity httpEntity;
    private KaskSessionStore<String> store;
    private HttpRequest lastExecutedRequest;


    @Before
    public void setUp() throws Exception {
        httpClient = mock(HttpClient.class);
        httpResponse = mock(HttpResponse.class);
        statusLine = mock(StatusLine.class);
        httpEntity = mock(HttpEntity.class);

        store = new KaskSessionStore<>(httpClient, httpHost, new KaskSessionStore.Serde<String>() {
            @Override
            public String keyEncoder(String key) {
                return key;
            }

            @Override
            public void valueEncoder(DataOutput out, String value) throws IOException {
                out.writeUTF(value);
            }

            @Override
            public String valueDecoder(DataInput in) throws IOException {
                return in.readUTF();
            }
        });
    }

    private <T extends HttpRequest> void mockResponse(Class<T> reqClazz, int statusCode) throws Exception {
        when(statusLine.getStatusCode()).thenReturn(statusCode);
        when(httpResponse.getStatusLine()).thenReturn(statusLine);
        when(httpClient.execute(eq(httpHost), isA(reqClazz), any(ResponseHandler.class))).then((args) -> {
            lastExecutedRequest = args.getArgumentAt(1, reqClazz);
            return args.getArgumentAt(2, ResponseHandler.class).handleResponse(httpResponse);
        });
    }

    private <T extends HttpRequest> void mockResponse(Class<T> reqClazz, int statusCode, InputStream response) throws Exception {
        when(httpEntity.getContent()).thenReturn(response);
        when(httpEntity.getContentLength()).thenReturn(-1L);
        when(httpResponse.getEntity()).thenReturn(httpEntity);
        mockResponse(reqClazz, statusCode);
    }

    private <T extends HttpRequest> void mockResponse(Class<T> reqClazz, int statusCode, String response) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeUTF(response);
        }
        mockResponse(reqClazz, statusCode, new ByteArrayInputStream(baos.toByteArray()));
    }

    @Test
    public void getReturnsNullWhenNotFound() throws Exception {
        mockResponse(HttpGet.class, 404);
        assertThat(store.getIfPresent("does_not_exist")).isNull();
    }

    @Test
    public void getReturnsExpectedValueWhenFound() throws Exception {
        String expected = "example";
        mockResponse(HttpGet.class, 200, expected);
        assertThat(store.getIfPresent("does_exist")).isEqualTo(expected);
    }

    @Test(expected = HttpResponseException.class)
    public void getThrowsOnFailure() throws Exception {
        mockResponse(HttpGet.class, 500);
        store.getIfPresent("should_throw");
    }

    @Test(expected = HttpResponseException.class)
    public void putThrowsOnFailure() throws Exception {
        mockResponse(HttpPost.class, 500);
        store.put("a", "b");
    }

    @Test
    public void putCompletesOnSuccess() throws Exception {
        mockResponse(HttpPost.class, 200);
        store.put("a", "b");
    }

    @Test
    public void canRoundTripEntities() throws Exception {
        String expected = "example content";

        mockResponse(HttpPost.class, 200);
        store.put("a", expected);
        InputStream content = ((HttpPost) lastExecutedRequest).getEntity().getContent();

        mockResponse(HttpGet.class, 200, content);
        assertThat(store.getIfPresent("a")).isEqualTo(expected);
    }

    @Test
    public void invalidateCompletesOnSuccess() throws Exception {
        mockResponse(HttpDelete.class, 200);
        store.invalidate("key");
    }

    @Test(expected = HttpResponseException.class)
    public void invalidateThrowsOnFailure() throws Exception {
        mockResponse(HttpDelete.class, 500);
        store.invalidate("should throw");
    }
}
