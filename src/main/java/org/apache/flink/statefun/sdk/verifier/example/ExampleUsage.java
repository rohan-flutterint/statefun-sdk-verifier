package org.apache.flink.statefun.sdk.verifier.example;

import org.apache.flink.statefun.sdk.verifier.SdkVerifier;

import java.net.URI;

public class ExampleUsage {

    public static void main(String[] args) {
        final SdkVerifier verifier = new SdkVerifier(URI.create("http://localhost:8080"));
        verifier.runFullTestSuite();
    }
}
