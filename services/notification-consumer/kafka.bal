import ballerinax/kafka;

final kafka:Producer eventProducer = check new (KAFKA_BOOTSTRAP, {
    clientId: SERVICE_NAME + "-producer",
    acks: "all",
    retryCount: 5,
    enableIdempotence: true
});

final kafka:Consumer notificationsConsumer = check new (KAFKA_BOOTSTRAP, {
    groupId: "notification-service-all",
    autoCommit: false,
    clientId: SERVICE_NAME + "-consumer"
});
