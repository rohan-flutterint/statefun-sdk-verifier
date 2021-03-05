package org.apache.flink.statefun.sdk.verifier;

import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.flink.statefun.sdk.shaded.com.google.protobuf.ByteString;

import static org.apache.flink.statefun.sdk.verifier.generated.Commands.Invoke;

final class Commands {

    private static final String COMMAND_TYPE_TYPENAME = "sdk.verification.types/" + Invoke.getDescriptor().getFullName();

    static Invoke sendToSelf() {
        return Invoke.newBuilder()
                .setCommand(Invoke.Command.SEND_TO_SELF)
                .build();
    }

    static Invoke sendToTarget() {
        return Invoke.newBuilder()
                .setCommand(Invoke.Command.SEND_TO_TARGET)
                .build();
    }

    static Invoke delayedSendToTarget() {
        return Invoke.newBuilder()
                .setCommand(Invoke.Command.DELAYED_SEND_TO_TARGET)
                .build();
    }

    static Invoke sendToKafkaEgress() {
        return Invoke.newBuilder()
                .setCommand(Invoke.Command.SEND_TO_KAFKA_EGRESS)
                .build();
    }

    static Invoke sendToKinesisEgress() {
        return Invoke.newBuilder()
                .setCommand(Invoke.Command.SEND_TO_KINESIS_EGRESS)
                .build();
    }

    static Invoke incrementIntStateA() {
        return Invoke.newBuilder()
                .setCommand(Invoke.Command.INCREMENT_INT_STATE_A)
                .build();
    }

    static Invoke toggleBooleanStateB() {
        return Invoke.newBuilder()
                .setCommand(Invoke.Command.TOGGLE_BOOL_STATE_B)
                .build();
    }

    static Invoke clearStringStateC() {
        return Invoke.newBuilder()
                .setCommand(Invoke.Command.CLEAR_STRING_STATE_C)
                .build();
    }

    static TypedValue asTypedValue(Invoke command) {
        return TypedValue.newBuilder()
                .setTypename(COMMAND_TYPE_TYPENAME)
                .setValue(ByteString.copyFrom(command.toByteArray()))
                .setHasValue(true)
                .build();
    }
}
