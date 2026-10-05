// Restaurant consumer. One group. Reacts to orders.confirmed by
// reserving stock, and to orders.cancelled by restoring it.

import ballerina/log;
import ballerinax/kafka;

final kafka:Producer eventProducer = check new (KAFKA_BOOTSTRAP, {
    clientId: SERVICE_NAME + "-producer",
    acks: "all",
    retryCount: 5,
    enableIdempotence: true
});

final kafka:Consumer ordersConsumer = check new (KAFKA_BOOTSTRAP, {
    groupId: "restaurant-service-orders",
    autoCommit: false,
    clientId: SERVICE_NAME + "-consumer"
});

public function publish(string topic, anydata payload, string key) returns error? {
    check eventProducer->send({
        topic: topic,
        value: payload,
        key: key.toBytes()
    });
    log:printDebug("event published", topic = topic, key = key);
}
