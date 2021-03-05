package org.apache.flink.statefun.sdk.verifier;

import org.apache.flink.statefun.sdk.egress.generated.KafkaProducerRecord;
import org.apache.flink.statefun.sdk.egress.generated.KinesisEgressRecord;
import org.apache.flink.statefun.sdk.reqreply.generated.TypedValue;
import org.apache.flink.statefun.sdk.shaded.com.google.protobuf.ByteString;
import org.apache.flink.statefun.sdk.types.generated.BooleanWrapper;
import org.apache.flink.statefun.sdk.types.generated.DoubleWrapper;
import org.apache.flink.statefun.sdk.types.generated.FloatWrapper;
import org.apache.flink.statefun.sdk.types.generated.IntWrapper;
import org.apache.flink.statefun.sdk.types.generated.LongWrapper;
import org.apache.flink.statefun.sdk.types.generated.StringWrapper;

final class TypedValues {

    static class TypeNames {
        public static String BOOLEAN = "io.statefun.types/bool";
        public static String INT = "io.statefun.types/int";
        public static String FLOAT = "io.statefun.types/float";
        public static String LONG = "io.statefun.types/long";
        public static String DOUBLE = "io.statefun.types/double";
        public static String STRING = "io.statefun.types/string";
        public static String KAFKA_EGRESS_MESSAGE = "type.googleapis.com/" + KafkaProducerRecord.getDescriptor().getFullName();
        public static String KINESIS_EGRESS_MESSAGE= "type.googleapis.com/" + KinesisEgressRecord.getDescriptor().getFullName();
    }

    static TypedValue forBoolean(boolean value) {
        final BooleanWrapper wrapper = BooleanWrapper.newBuilder().setValue(value).build();
        return forCustomType(TypeNames.BOOLEAN, wrapper.toByteString());
    }

    static TypedValue forInt(int value) {
        final IntWrapper wrapper = IntWrapper.newBuilder().setValue(value).build();
        return forCustomType(TypeNames.INT, wrapper.toByteString());
    }

    static TypedValue forFloat(float value) {
        final FloatWrapper wrapper = FloatWrapper.newBuilder().setValue(value).build();
        return forCustomType(TypeNames.FLOAT, wrapper.toByteString());
    }

    static TypedValue forLong(long value) {
        final LongWrapper wrapper = LongWrapper.newBuilder().setValue(value).build();
        return forCustomType(TypeNames.LONG, wrapper.toByteString());
    }

    static TypedValue forDouble(double value) {
        final DoubleWrapper wrapper = DoubleWrapper.newBuilder().setValue(value).build();
        return forCustomType(TypeNames.DOUBLE, wrapper.toByteString());
    }

    static TypedValue forString(String value) {
        final StringWrapper wrapper = StringWrapper.newBuilder().setValue(value).build();
        return forCustomType(TypeNames.STRING, wrapper.toByteString());
    }

    static TypedValue forKafkaProducerRecord(String topic, String key, ByteString value) {
        final KafkaProducerRecord record = KafkaProducerRecord.newBuilder()
                .setKey(key)
                .setValueBytes(value)
                .setTopic(topic)
                .build();

        return forCustomType(TypeNames.KAFKA_EGRESS_MESSAGE, record.toByteString());
    }

    static TypedValue forKinesisProducerRecord(String streamName, String partitionKey, String explicitHashKey, ByteString value) {
        final KinesisEgressRecord record = KinesisEgressRecord.newBuilder()
                .setStream(streamName)
                .setPartitionKey(partitionKey)
                .setExplicitHashKey(explicitHashKey)
                .setValueBytes(value)
                .build();

        return forCustomType(TypeNames.KINESIS_EGRESS_MESSAGE, record.toByteString());
    }

    static TypedValue forCustomType(String typename, ByteString bytes) {
        return TypedValue.newBuilder()
                .setTypename(typename)
                .setValue(bytes)
                .setHasValue(true)
                .build();
    }

    static TypedValue forCustomType(String typename, byte[] bytes) {
        return forCustomType(typename, ByteString.copyFrom(bytes));
    }

    static TypedValue unsetValue(String typename) {
        return TypedValue.newBuilder()
                .setTypename(typename)
                .build();
    }
}
