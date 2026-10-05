// Orders consumer group. Admin-service only reads the event stream to
// build its read model; it never publishes.

import ballerinax/kafka;

final kafka:Consumer ordersConsumer = check new (KAFKA_BOOTSTRAP, {
    groupId: "admin-service-orders",
    autoCommit: false,
    clientId: SERVICE_NAME + "-orders"
});
