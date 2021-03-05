package org.apache.flink.statefun.sdk.verifier;

import static org.apache.flink.statefun.sdk.verifier.ProtocolMessages.address;
import static org.apache.flink.statefun.sdk.verifier.ProtocolMessages.expireAfterInvoke;
import static org.apache.flink.statefun.sdk.verifier.ProtocolMessages.expireAfterWrite;
import static org.apache.flink.statefun.sdk.verifier.ProtocolMessages.incompleteInvocationContextResponseBuilder;
import static org.apache.flink.statefun.sdk.verifier.ProtocolMessages.requestBuilder;
import static org.apache.flink.statefun.sdk.verifier.ProtocolMessages.successResponseBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import org.apache.flink.statefun.sdk.reqreply.generated.Address;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.ToFunction;
import org.apache.flink.statefun.sdk.verifier.generated.Commands.Invoke;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public final class SdkVerifier {

    private static final Logger LOG = LoggerFactory.getLogger(SdkVerifier.class);

    private static final Address CALLER_ADDRESS = address("sdk.verification.functions", "caller", "id");

    private final InvocationHttpClient client;

    public SdkVerifier(URI functionEndpointUrl) {
        this.client = new InvocationHttpClient(functionEndpointUrl);
    }

    public void runFullTestSuite() {
        validateTypeSystem();
        validateMessaging();
        validateState();
        sendBatchOfCommands();
    }

    public void validateTypeSystem() {
        LOG.info("==============================================================");
        LOG.info(" Stateful Functions primitive types validations");
        LOG.info("==============================================================");

        toggleBooleanAndReply();
        incrementIntAndReply();
        multipleFloatAndReply();
        multipleLongAndReply();
        multiplyDoubleAndReply();
        lowercaseStringAndReply();
    }

    private static final Address TYPE_SYSTEM_VALIDATION_FN_ADDRESS = address("sdk.verification.functions", "types-validation", "foobar");

    public void toggleBooleanAndReply() {
        final ToFunction request = requestBuilder()
                .withTarget(TYPE_SYSTEM_VALIDATION_FN_ADDRESS)
                .withInvocationFromFunction(TypedValues.forBoolean(true), CALLER_ADDRESS)
                .build();

        final FromFunction expectedResponse = successResponseBuilder()
                .withInvocation(TypedValues.forBoolean(false), CALLER_ADDRESS)
                .build();

        final FromFunction response = client.post(request);
        assertThat(response, isSuccessfulResponse(expectedResponse));
        logTestCase("toggleBooleanAndReply", request, response);
    }

    public void incrementIntAndReply() {
        final ToFunction request = requestBuilder()
                .withTarget(TYPE_SYSTEM_VALIDATION_FN_ADDRESS)
                .withInvocationFromFunction(TypedValues.forInt(1108), CALLER_ADDRESS)
                .build();

        final FromFunction expectedResponse = successResponseBuilder()
                .withInvocation(TypedValues.forInt(1109), CALLER_ADDRESS)
                .build();

        final FromFunction response = client.post(request);
        assertThat(response, isSuccessfulResponse(expectedResponse));
        logTestCase("incrementIntAndReply", request, response);
    }

    public void multipleFloatAndReply() {
        final float testValue = 3.14159e+11f;

        final ToFunction request = requestBuilder()
                .withTarget(TYPE_SYSTEM_VALIDATION_FN_ADDRESS)
                .withInvocationFromFunction(TypedValues.forFloat(testValue), CALLER_ADDRESS)
                .build();

        final FromFunction expectedResponse = successResponseBuilder()
                .withInvocation(TypedValues.forFloat(testValue * 3), CALLER_ADDRESS)
                .build();

        final FromFunction response = client.post(request);
        assertThat(response, isSuccessfulResponse(expectedResponse));
        logTestCase("multipleFloatAndReply", request, response);
    }

    public void multiplyDoubleAndReply() {
        final double testValue = 3.14159;

        final ToFunction request = requestBuilder()
                .withTarget(TYPE_SYSTEM_VALIDATION_FN_ADDRESS)
                .withInvocationFromFunction(TypedValues.forDouble(testValue), CALLER_ADDRESS)
                .build();

        final FromFunction expectedResponse = successResponseBuilder()
                .withInvocation(TypedValues.forDouble(testValue * 7), CALLER_ADDRESS)
                .build();

        final FromFunction response = client.post(request);
        assertThat(response, isSuccessfulResponse(expectedResponse));
        logTestCase("multiplyDoubleAndReply", request, response);
    }

    public void multipleLongAndReply() {
        final long testValue = -19911108123046639L;

        final ToFunction request = requestBuilder()
                .withTarget(TYPE_SYSTEM_VALIDATION_FN_ADDRESS)
                .withInvocationFromFunction(TypedValues.forLong(testValue), CALLER_ADDRESS)
                .build();

        final FromFunction expectedResponse = successResponseBuilder()
                .withInvocation(TypedValues.forLong(testValue * 9), CALLER_ADDRESS)
                .build();

        final FromFunction response = client.post(request);
        assertThat(response, isSuccessfulResponse(expectedResponse));
        logTestCase("multipleLongAndReply", request, response);
    }

    public void lowercaseStringAndReply() {
        final ToFunction request = requestBuilder()
                .withTarget(TYPE_SYSTEM_VALIDATION_FN_ADDRESS)
                .withInvocationFromFunction(TypedValues.forString("hElLO WoRLd!"), CALLER_ADDRESS)
                .build();

        final FromFunction expectedResponse = successResponseBuilder()
                .withInvocation(TypedValues.forString("hello world!"), CALLER_ADDRESS)
                .build();

        final FromFunction response = client.post(request);
        assertThat(response, isSuccessfulResponse(expectedResponse));
        logTestCase("lowercaseStringAndReply", request, response);
    }

    // ==========================================================================
    //  Sending messages
    // ==========================================================================

    public void validateMessaging() {
        LOG.info("==============================================================");
        LOG.info("  Messaging primitive validations");
        LOG.info("==============================================================");

        sendToSelf();
        sendToTargetFunction();
        delayedSendToTargetFunction();
        sendKafkaEgress();
        sendKinesisEgress();
    }

    private static final Address COMMAND_INTERPRETER_FN_ADDRESS = address("sdk.verification.functions", "command-interpreter", "foobar");
    private static final Address TARGET_FN_ADDRESS = address("target.namespace", "target.function", "target.id");

    public void sendToSelf() {
        final Invoke command = Commands.sendToSelf();
        final ToFunction request = requestBuilder()
                .withTarget(COMMAND_INTERPRETER_FN_ADDRESS)
                .withState("state-a", TypedValues.unsetValue(TypedValues.TypeNames.INT))
                .withState("state-b", TypedValues.unsetValue(TypedValues.TypeNames.BOOLEAN))
                .withState("state-c", TypedValues.unsetValue(TypedValues.TypeNames.STRING))
                .withInvocationFromFunction(Commands.asTypedValue(command), CALLER_ADDRESS)
                .build();

        final FromFunction expectedResponse = successResponseBuilder()
                .withInvocation(TypedValues.forString("hello world!"), COMMAND_INTERPRETER_FN_ADDRESS)
                .build();

        final FromFunction response = client.post(request);
        assertThat(response, isSuccessfulResponse(expectedResponse));
        logTestCase("sendToSelf", request, response);
    }

    public void sendToTargetFunction() {
        final Invoke command = Commands.sendToTarget();
        final ToFunction request = requestBuilder()
                .withTarget(COMMAND_INTERPRETER_FN_ADDRESS)
                .withState("state-a", TypedValues.unsetValue(TypedValues.TypeNames.INT))
                .withState("state-b", TypedValues.unsetValue(TypedValues.TypeNames.BOOLEAN))
                .withState("state-c", TypedValues.unsetValue(TypedValues.TypeNames.STRING))
                .withInvocationFromFunction(Commands.asTypedValue(command), CALLER_ADDRESS)
                .build();

        final FromFunction expectedResponse = successResponseBuilder()
                .withInvocation(TypedValues.forString("hello world!"), TARGET_FN_ADDRESS)
                .build();

        final FromFunction response = client.post(request);
        assertThat(response, isSuccessfulResponse(expectedResponse));
        logTestCase("sendToTargetFunction", request, response);
    }

    public void delayedSendToTargetFunction() {
        final Invoke command = Commands.delayedSendToTarget();
        final ToFunction request = requestBuilder()
                .withTarget(COMMAND_INTERPRETER_FN_ADDRESS)
                .withState("state-a", TypedValues.unsetValue(TypedValues.TypeNames.INT))
                .withState("state-b", TypedValues.unsetValue(TypedValues.TypeNames.BOOLEAN))
                .withState("state-c", TypedValues.unsetValue(TypedValues.TypeNames.STRING))
                .withInvocationFromFunction(Commands.asTypedValue(command), CALLER_ADDRESS)
                .build();

        final FromFunction expectedResponse = successResponseBuilder()
                .withDelayedInvocations(
                        TypedValues.forString("hello world!"),
                        TARGET_FN_ADDRESS,
                        Duration.ofSeconds(100))
                .build();

        final FromFunction response = client.post(request);
        assertThat(response, isSuccessfulResponse(expectedResponse));
        logTestCase("sendToTargetFunction", request, response);
    }

    public void sendKafkaEgress() {
        final Invoke command = Commands.sendToKafkaEgress();
        final ToFunction request = requestBuilder()
                .withTarget(COMMAND_INTERPRETER_FN_ADDRESS)
                .withState("state-a", TypedValues.unsetValue(TypedValues.TypeNames.INT))
                .withState("state-b", TypedValues.unsetValue(TypedValues.TypeNames.BOOLEAN))
                .withState("state-c", TypedValues.unsetValue(TypedValues.TypeNames.STRING))
                .withInvocationFromFunction(Commands.asTypedValue(command), CALLER_ADDRESS)
                .build();

        final FromFunction expectedResponse = successResponseBuilder()
                .withEgressMessage(
                        TypedValues.forKafkaProducerRecord("target.topic", "my-key", TypedValues.forInt(1108).getValue()),
                        "target.namespace",
                        "target.kafka")
                .build();

        final FromFunction response = client.post(request);
        assertThat(response, isSuccessfulResponse(expectedResponse));
        logTestCase("sendKafkaEgress", request, response);
    }

    public void sendKinesisEgress() {
        final Invoke command = Commands.sendToKinesisEgress();
        final ToFunction request = requestBuilder()
                .withTarget(COMMAND_INTERPRETER_FN_ADDRESS)
                .withState("state-a", TypedValues.unsetValue(TypedValues.TypeNames.INT))
                .withState("state-b", TypedValues.unsetValue(TypedValues.TypeNames.BOOLEAN))
                .withState("state-c", TypedValues.unsetValue(TypedValues.TypeNames.STRING))
                .withInvocationFromFunction(Commands.asTypedValue(command), CALLER_ADDRESS)
                .build();

        final FromFunction expectedResponse = successResponseBuilder()
                .withEgressMessage(
                        TypedValues.forKinesisProducerRecord(
                                "target.stream",
                                "my-key",
                                "my-explicit-hash-key",
                                TypedValues.forString("hello world!").getValue()),
                        "target.namespace",
                        "target.kinesis")
                .build();

        final FromFunction response = client.post(request);
        assertThat(response, isSuccessfulResponse(expectedResponse));
        logTestCase("sendKinesisEgress", request, response);
    }

    // ==========================================================================
    //  State
    // ==========================================================================

    public void validateState() {
        LOG.info("==============================================================");
        LOG.info("  State protocol validations");
        LOG.info("==============================================================");

        incompleteInvocationContext();
        partiallyIncompleteInvocationContext();
        invokeWithExcessiveState();
        incrementStateA();
        toggleStateB();
        clearStateC();
    }

    public void incompleteInvocationContext() {
        final Invoke command = Commands.sendToSelf();
        final ToFunction request = requestBuilder()
                .withTarget(COMMAND_INTERPRETER_FN_ADDRESS)
                .withInvocationFromFunction(Commands.asTypedValue(command), CALLER_ADDRESS)
                .build();

        final FromFunction expectedResponse = incompleteInvocationContextResponseBuilder()
                .withMissingValueSpec("state-a", TypedValues.TypeNames.INT)
                .withMissingValueSpec("state-b", TypedValues.TypeNames.BOOLEAN, expireAfterWrite(Duration.ofDays(1)))
                .withMissingValueSpec("state-c", TypedValues.TypeNames.STRING, expireAfterInvoke(Duration.ofMinutes(5)))
                .build();

        final FromFunction response = client.post(request);
        assertThat(response, isIncompleteInvocationContextResponse(expectedResponse));
        logTestCase("incompleteInvocationContext", request, response);
    }

    public void partiallyIncompleteInvocationContext() {
        final Invoke command = Commands.sendToSelf();
        final ToFunction request = requestBuilder()
                .withTarget(COMMAND_INTERPRETER_FN_ADDRESS)
                .withState("state-b", TypedValues.forBoolean(false))
                .withInvocationFromFunction(Commands.asTypedValue(command), CALLER_ADDRESS)
                .build();

        final FromFunction expectedResponse = incompleteInvocationContextResponseBuilder()
                .withMissingValueSpec("state-a", TypedValues.TypeNames.INT)
                .withMissingValueSpec("state-c", TypedValues.TypeNames.STRING, expireAfterInvoke(Duration.ofMinutes(5)))
                .build();

        final FromFunction response = client.post(request);
        assertThat(response, isIncompleteInvocationContextResponse(expectedResponse));
        logTestCase("partiallyIncompleteInvocationContext", request, response);
    }

    public void invokeWithExcessiveState() {
        final Invoke command = Commands.sendToSelf();
        final ToFunction request = requestBuilder()
                .withTarget(COMMAND_INTERPRETER_FN_ADDRESS)
                .withState("state-a", TypedValues.forInt(-1))
                .withState("state-b", TypedValues.forBoolean(true))
                .withState("state-c", TypedValues.forString("hello world!"))
                .withState("non-registered-state", TypedValues.forLong(1108L))
                .withInvocationFromFunction(Commands.asTypedValue(command), CALLER_ADDRESS)
                .build();

        final FromFunction expectedResponse = successResponseBuilder()
                .withInvocation(TypedValues.forString("hello world!"), COMMAND_INTERPRETER_FN_ADDRESS)
                .build();

        final FromFunction response = client.post(request);
        assertThat(response, isSuccessfulResponse(expectedResponse));
        logTestCase("invokeWithExcessiveState", request, response);
    }

    public void incrementStateA() {
        final Invoke command = Commands.incrementIntStateA();
        final ToFunction request = requestBuilder()
                .withTarget(COMMAND_INTERPRETER_FN_ADDRESS)
                .withState("state-a", TypedValues.forInt(1108))
                .withState("state-b", TypedValues.unsetValue(TypedValues.TypeNames.BOOLEAN))
                .withState("state-c", TypedValues.unsetValue(TypedValues.TypeNames.STRING))
                .withInvocationFromFunction(Commands.asTypedValue(command), CALLER_ADDRESS)
                .build();

        final FromFunction expectedResponse = successResponseBuilder()
                .withMutatedState("state-a", TypedValues.forInt(1109))
                .build();

        final FromFunction response = client.post(request);
        assertThat(response, isSuccessfulResponse(expectedResponse));
        logTestCase("incrementStateA", request, response);
    }

    public void toggleStateB() {
        final Invoke command = Commands.toggleBooleanStateB();
        final ToFunction request = requestBuilder()
                .withTarget(COMMAND_INTERPRETER_FN_ADDRESS)
                .withState("state-a", TypedValues.unsetValue(TypedValues.TypeNames.INT))
                .withState("state-b", TypedValues.forBoolean(true))
                .withState("state-c", TypedValues.unsetValue(TypedValues.TypeNames.STRING))
                .withInvocationFromFunction(Commands.asTypedValue(command), CALLER_ADDRESS)
                .build();

        final FromFunction expectedResponse = successResponseBuilder()
                .withMutatedState("state-b", TypedValues.forBoolean(false))
                .build();

        final FromFunction response = client.post(request);
        assertThat(response, isSuccessfulResponse(expectedResponse));
        logTestCase("toggleStateB", request, response);
    }

    public void clearStateC() {
        final Invoke command = Commands.clearStringStateC();
        final ToFunction request = requestBuilder()
                .withTarget(COMMAND_INTERPRETER_FN_ADDRESS)
                .withState("state-a", TypedValues.unsetValue(TypedValues.TypeNames.INT))
                .withState("state-b", TypedValues.unsetValue(TypedValues.TypeNames.BOOLEAN))
                .withState("state-c", TypedValues.forString("hello world!"))
                .withInvocationFromFunction(Commands.asTypedValue(command), CALLER_ADDRESS)
                .build();

        final FromFunction expectedResponse = successResponseBuilder()
                .withClearedState("state-c")
                .build();

        final FromFunction response = client.post(request);
        assertThat(response, isSuccessfulResponse(expectedResponse));
        logTestCase("clearStateC", request, response);
    }

    public void sendBatchOfCommands() {
        final ToFunction request = requestBuilder()
                .withTarget(COMMAND_INTERPRETER_FN_ADDRESS)
                .withState("state-a", TypedValues.forInt(-1))
                .withState("state-b", TypedValues.forBoolean(false))
                .withState("state-c", TypedValues.forString("hello world!"))
                .withInvocationFromFunction(
                        Commands.asTypedValue(Commands.sendToKafkaEgress()), CALLER_ADDRESS)
                .withInvocationFromFunction(
                        Commands.asTypedValue(Commands.incrementIntStateA()), CALLER_ADDRESS)
                .withInvocationFromFunction(
                        Commands.asTypedValue(Commands.clearStringStateC()), CALLER_ADDRESS)
                .withInvocationFromFunction(
                        Commands.asTypedValue(Commands.sendToSelf()), CALLER_ADDRESS)
                .withInvocationFromFunction(
                        Commands.asTypedValue(Commands.sendToKinesisEgress()), CALLER_ADDRESS)
                .withInvocationFromFunction(
                        Commands.asTypedValue(Commands.incrementIntStateA()), CALLER_ADDRESS)
                .withInvocationFromFunction(
                        Commands.asTypedValue(Commands.delayedSendToTarget()), CALLER_ADDRESS)
                .withInvocationFromFunction(
                        Commands.asTypedValue(Commands.toggleBooleanStateB()), CALLER_ADDRESS)
                .withInvocationFromFunction(
                        Commands.asTypedValue(Commands.toggleBooleanStateB()), CALLER_ADDRESS)
                .withInvocationFromFunction(
                        Commands.asTypedValue(Commands.sendToTarget()), CALLER_ADDRESS)
                .build();

        final FromFunction expectedResponse = successResponseBuilder()
                .withEgressMessage(
                        TypedValues.forKafkaProducerRecord("target.topic", "my-key", TypedValues.forInt(1108).getValue()),
                        "target.namespace",
                        "target.kafka")
                .withInvocation(TypedValues.forString("hello world!"), COMMAND_INTERPRETER_FN_ADDRESS)
                .withEgressMessage(
                        TypedValues.forKinesisProducerRecord(
                                "target.stream",
                                "my-key",
                                "my-explicit-hash-key",
                                TypedValues.forString("hello world!").getValue()),
                        "target.namespace",
                        "target.kinesis")
                .withInvocation(TypedValues.forString("hello world!"), TARGET_FN_ADDRESS)
                .withDelayedInvocations(TypedValues.forString("hello world!"), TARGET_FN_ADDRESS, Duration.ofSeconds(100))
                .withMutatedState("state-a", TypedValues.forInt(1))
                .withMutatedState("state-b", TypedValues.forBoolean(false))
                .withClearedState("state-c")
                .build();

        final FromFunction response = client.post(request);
        assertThat(response, isSuccessfulResponse(expectedResponse));
        logTestCase("sendBatchOfCommands", request, response);
    }

    private static void logTestCase(String testName, ToFunction sent, FromFunction received) {
        LOG.info("Test: {}\nSENT: {}\nRECEIVED: {}", testName, sent, received);
    }

    private static Matcher<FromFunction> isIncompleteInvocationContextResponse(FromFunction expected) {
        return new TypeSafeMatcher<FromFunction>() {
            @Override
            protected boolean matchesSafely(FromFunction received) {
                final List<FromFunction.PersistedValueSpec> missingValueSpecs =
                        received.getIncompleteInvocationContext().getMissingValuesList();

                final Set<FromFunction.PersistedValueSpec> specsToMatch = new HashSet<>(
                        expected.getIncompleteInvocationContext().getMissingValuesList());

                for (FromFunction.PersistedValueSpec spec : missingValueSpecs) {
                    if (!specsToMatch.remove(spec)) {
                        return false;
                    }
                }
                return specsToMatch.isEmpty();
            }

            @Override
            public void describeTo(Description description) {}
        };
    }

    private static Matcher<FromFunction> isSuccessfulResponse(FromFunction expected) {
        return new TypeSafeMatcher<FromFunction>() {
            @Override
            protected boolean matchesSafely(FromFunction received) {
                final FromFunction.InvocationResponse expectedResponse = expected.getInvocationResult();
                final FromFunction.InvocationResponse response = received.getInvocationResult();

                final boolean outgoingMessagesMatches = Objects.equals(response.getOutgoingMessagesList(), expectedResponse.getOutgoingMessagesList())
                        && Objects.equals(response.getDelayedInvocationsList(), expectedResponse.getDelayedInvocationsList())
                        && Objects.equals(response.getOutgoingEgressesList(), expectedResponse.getOutgoingEgressesList());

                final boolean stateMutationsMatches = (expectedResponse.getStateMutationsList() == null)
                        ? response.getStateMutationsList() == null
                        : stateMutationsMatchesInAnyOrder(
                                response.getStateMutationsList(),
                                expectedResponse.getStateMutationsList());

                return outgoingMessagesMatches && stateMutationsMatches;
            }

            @Override
            public void describeTo(Description description) {}

            private boolean stateMutationsMatchesInAnyOrder(
                    List<FromFunction.PersistedValueMutation> responseStateMutations,
                    List<FromFunction.PersistedValueMutation> expectedStateMutations) {
                final Set<FromFunction.PersistedValueMutation> toMatch = new HashSet<>(expectedStateMutations);

                for (FromFunction.PersistedValueMutation mutation : responseStateMutations) {
                    if (!toMatch.remove(mutation)) {
                        return false;
                    }
                }
                return toMatch.isEmpty();
            }
        };
    }
}
