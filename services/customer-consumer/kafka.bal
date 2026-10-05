import ballerinax/kafka;

final kafka:Consumer ordersConsumer = check new (KAFKA_BOOTSTRAP, {
    groupId: "customer-service-orders",
    autoCommit: false,
    clientId: SERVICE_NAME + "-consumer"
});
