import ballerinax/kafka;

final kafka:Consumer deliveryConsumer = check new (KAFKA_BOOTSTRAP, {
    groupId: "admin-service-delivery",
    autoCommit: false,
    clientId: SERVICE_NAME + "-delivery"
});
