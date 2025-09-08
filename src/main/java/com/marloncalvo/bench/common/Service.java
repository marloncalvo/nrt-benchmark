package com.marloncalvo.bench.common;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;

public class Service {
    private static final String endpointBasePath = System.getenv().getOrDefault("ENDPOINT_BASE_PATH", "http://go-server:5050");

    private final HttpClient javaHttpClient;

    private final CloseableHttpClient httpClient;
    private final RequestConfig requestConfig;

    public Service() {
        this.javaHttpClient = HttpClient.newBuilder()
            .connectTimeout(java.time.Duration.ofMillis(2000))
            .executor(Executors.newVirtualThreadPerTaskExecutor())
            .build();

        this.httpClient = HttpClients.custom()
            .disableAutomaticRetries()
            .disableContentCompression()
            .build();
        this.requestConfig = RequestConfig.custom()
            .setResponseTimeout(2000, TimeUnit.MILLISECONDS)
            .build();
    }

    public void get() {
        this.getJava();
    }

    public CompletableFuture<?> getAsync() {
        return this.getJavaAsync();
    }

    private CompletableFuture<HttpResponse<Void>> getJavaAsync() {
        return this.javaHttpClient.sendAsync(
            HttpRequest.newBuilder()
                .uri(URI.create(endpointBasePath + "/"))
                .timeout(java.time.Duration.ofMillis(200))
                .build(),
            HttpResponse.BodyHandlers.discarding()
        );
    }

    private void getJava() {
        try {
			this.javaHttpClient.send(HttpRequest.newBuilder()
			        .uri(URI.create(endpointBasePath + "/"))
			        .timeout(java.time.Duration.ofMillis(200))
			        .build(), HttpResponse.BodyHandlers.discarding());
		} catch (Exception e) {
            // Handle exception
        }
    }

    private void getApache() {
        try {
            HttpGet request = new HttpGet(new URI(endpointBasePath + "/"));
            request.setConfig(requestConfig);
            httpClient.execute(request, response -> {
                if (response.getCode() == 200) {
                    // Handle successful response
                } else {
                    // Handle error response
                }
                return null;
            });
        } catch (Exception e) {
            // Handle exception
        }
    }
}
