// The Kafka producer. Shared by every publisher in this service.
//
// acks=all plus enableIdempotence gives the producer exactly-once delivery
// per partition. A retry after a network failure cannot duplicate the record.
//
// Consumers still commit offsets manually, so end-to-end delivery is
// at-least-once. Every handler is written to be idempotent for that reason.

import ballerina/log;
import ballerinax/kafka;

final kafka:Producer eventProducer = check new (KAFKA_BOOTSTRAP, {
    clientId: SERVICE_NAME + "-producer",
    acks: "all",
    retryCount: 5,
    enableIdempotence: true
});

public function publish(string topic, anydata payload, string key) returns error? {
    check eventProducer->send({
        topic: topic,
        value: payload,
        key: key.toBytes()
    });
    log:printDebug("event published", topic = topic, key = key);
}
