package org.apache.flink.statefun.sdk.verifier;

import org.apache.flink.statefun.sdk.reqreply.generated.Address;
import org.apache.flink.statefun.sdk.reqreply.generated.FromFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.ToFunction;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;

import java.time.Duration;

final class ProtocolMessages {

    static Address address(String namespace, String name, String id) {
        return Address.newBuilder().setNamespace(namespace).setType(name).setId(id).build();
    }

    static FromFunction.ExpirationSpec expireAfterWrite(Duration ttl) {
        return FromFunction.ExpirationSpec.newBuilder()
                .setMode(FromFunction.ExpirationSpec.ExpireMode.AFTER_WRITE)
                .setExpireAfterMillis(ttl.toMillis())
                .build();
    }

    static FromFunction.ExpirationSpec expireAfterInvoke(Duration ttl) {
        return FromFunction.ExpirationSpec.newBuilder()
                .setMode(FromFunction.ExpirationSpec.ExpireMode.AFTER_INVOKE)
                .setExpireAfterMillis(ttl.toMillis())
                .build();
    }

    static RequestBuilder requestBuilder() {
        return new RequestBuilder();
    }

    static InvocationResponseBuilder successResponseBuilder() {
        return new InvocationResponseBuilder();
    }

    static IncompleteInvocationContextResponseBuilder incompleteInvocationContextResponseBuilder() {
        return new IncompleteInvocationContextResponseBuilder();
    }

    static class RequestBuilder {
        private final ToFunction.InvocationBatchRequest.Builder builder =
                ToFunction.InvocationBatchRequest.newBuilder();

        public RequestBuilder withTarget(Address address) {
            builder.setTarget(address);
            return this;
        }

        public RequestBuilder withState(String stateName, TypedValue value) {
            builder.addState(
                    ToFunction.PersistedValue.newBuilder().setStateName(stateName).setStateValue(value));

            return this;
        }

        public RequestBuilder withInvocationFromIngress(TypedValue value) {
            builder.addInvocations(ToFunction.Invocation.newBuilder().setArgument(value));
            return this;
        }

        public RequestBuilder withInvocationFromFunction(TypedValue value, Address caller) {
            builder.addInvocations(ToFunction.Invocation.newBuilder().setCaller(caller).setArgument(value).build());
            return this;
        }

        public ToFunction build() {
            return ToFunction.newBuilder().setInvocation(builder).build();
        }
    }

    static class InvocationResponseBuilder {
        private final FromFunction.InvocationResponse.Builder builder =
                FromFunction.InvocationResponse.newBuilder();

        public InvocationResponseBuilder withInvocation(
                TypedValue argument, Address target) {
            builder.addOutgoingMessages(
                    FromFunction.Invocation.newBuilder()
                            .setTarget(target)
                            .setArgument(argument));
            return this;
        }

        public InvocationResponseBuilder withDelayedInvocations(
                TypedValue argument, Address target, Duration delay) {
            builder.addDelayedInvocations(
                    FromFunction.DelayedInvocation.newBuilder()
                            .setDelayInMs(delay.toMillis())
                            .setTarget(target)
                            .setArgument(argument));
            return this;
        }

        public InvocationResponseBuilder withEgressMessage(
                TypedValue message, String egressNamespace, String egressName) {
            builder.addOutgoingEgresses(
                    FromFunction.EgressMessage.newBuilder()
                            .setArgument(message)
                            .setEgressNamespace(egressNamespace)
                            .setEgressType(egressName)
                            .build());
            return this;
        }

        public InvocationResponseBuilder withMutatedState(
                String stateName, TypedValue newValue) {
            builder.addStateMutations(
                    FromFunction.PersistedValueMutation.newBuilder()
                            .setMutationType(FromFunction.PersistedValueMutation.MutationType.MODIFY)
                            .setStateName(stateName)
                            .setStateValue(newValue)
                            .build());
            return this;
        }

        public InvocationResponseBuilder withClearedState(String stateName) {
            builder.addStateMutations(
                    FromFunction.PersistedValueMutation.newBuilder()
                            .setMutationType(FromFunction.PersistedValueMutation.MutationType.DELETE)
                            .setStateName(stateName)
                            .build());
            return this;
        }

        public FromFunction build() {
            return FromFunction.newBuilder().setInvocationResult(builder).build();
        }
    }

    static class IncompleteInvocationContextResponseBuilder {
        private final FromFunction.IncompleteInvocationContext.Builder builder =
                FromFunction.IncompleteInvocationContext.newBuilder();

        public IncompleteInvocationContextResponseBuilder withMissingValueSpec(String stateName, String typename) {
            builder.addMissingValues(
                    FromFunction.PersistedValueSpec.newBuilder()
                            .setStateName(stateName)
                            .setTypeTypename(typename)
                            .build());
            return this;
        }

        public IncompleteInvocationContextResponseBuilder withMissingValueSpec(String stateName, String typeName, FromFunction.ExpirationSpec ttl) {
            builder.addMissingValues(
                    FromFunction.PersistedValueSpec.newBuilder()
                            .setStateName(stateName)
                            .setTypeTypename(typeName)
                            .setExpirationSpec(ttl)
                            .build());
            return this;
        }

        public FromFunction build() {
            return FromFunction.newBuilder().setIncompleteInvocationContext(builder).build();
        }
    }
}
