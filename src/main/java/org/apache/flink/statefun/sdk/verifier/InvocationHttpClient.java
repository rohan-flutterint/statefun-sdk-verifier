package org.apache.flink.statefun.sdk.verifier;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;

import okhttp3.Call;
import okhttp3.ConnectionPool;
import okhttp3.Dispatcher;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.ToFunction;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.concurrent.TimeUnit;

final class InvocationHttpClient {

    private static final MediaType MEDIA_TYPE_BINARY = MediaType.parse("application/octet-stream");

    private final HttpUrl endpointUrl;
    private final OkHttpClient client;

    InvocationHttpClient(URI functionEndpointUrl) {
        this.endpointUrl = HttpUrl.get(functionEndpointUrl);
        this.client = createClient();
    }

    FromFunction post(ToFunction request) {
        Request httpRequest =
                new Request.Builder()
                        .url(endpointUrl)
                        .post(RequestBody.create(MEDIA_TYPE_BINARY, request.toByteArray()))
                        .build();

        Call newCall = client.newCall(httpRequest);

        try {
            return parseResponse(newCall.execute());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static FromFunction parseResponse(Response response) throws IOException {
        try (InputStream httpResponseBody = responseBody(response)) {
            return FromFunction.parseFrom(httpResponseBody);
        }
    }

    private static InputStream responseBody(Response httpResponse) {
        assertThat(httpResponse.isSuccessful(), is(true));
        assertThat(httpResponse.body(), notNullValue());
        assertThat(httpResponse.body().contentType(), is(MEDIA_TYPE_BINARY));

        //checkState(httpResponse.isSuccessful(), "Unexpected HTTP status code %s", httpResponse.code());
        //checkState(httpResponse.body() != null, "Unexpected empty HTTP response (no body)");
        /*
        checkState(
                Objects.equals(httpResponse.body().contentType(), MEDIA_TYPE_BINARY),
                "Wrong HTTP content-type %s",
                httpResponse.body().contentType());
                */
        return httpResponse.body().byteStream();
    }

    private static OkHttpClient createClient() {
        final Dispatcher dispatcher = new Dispatcher();
        dispatcher.setMaxRequestsPerHost(Integer.MAX_VALUE);
        dispatcher.setMaxRequests(Integer.MAX_VALUE);

        final ConnectionPool connectionPool = new ConnectionPool(1024, 1, TimeUnit.MINUTES);

        return new OkHttpClient.Builder()
                .dispatcher(dispatcher)
                .connectionPool(connectionPool)
                .followRedirects(true)
                .followSslRedirects(true)
                .retryOnConnectionFailure(true)
                .build();
    }
}
