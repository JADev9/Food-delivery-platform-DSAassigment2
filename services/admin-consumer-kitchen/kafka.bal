import ballerinax/kafka;

final kafka:Consumer kitchenConsumer = check new (KAFKA_BOOTSTRAP, {
    groupId: "admin-service-kitchen",
    autoCommit: false,
    clientId: SERVICE_NAME + "-kitchen"
});
