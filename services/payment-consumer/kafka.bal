// acks=all plus enableIdempotence makes the producer safe to retry.
// Consumers commit offsets manually, so delivery is at-least-once and
// every handler is written to be idempotent.

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
