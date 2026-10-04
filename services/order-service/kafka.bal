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

// Publishes an event, keyed by orderId so that all events for one order
// land on the same partition and are consumed in the order they were
// produced.
//
// + topic - destination topic
// + payload - event record, serialised to JSON by the Kafka module
// + key - partition key, always the orderId
// + return - an error if the broker rejected the record
public function publish(string topic, anydata payload, string key) returns error? {
    check eventProducer->send({
        topic: topic,
        value: payload,
        key: key.toBytes()
    });
    log:printDebug("event published", topic = topic, key = key);
}
