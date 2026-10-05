import ballerinax/mongodb;

final mongodb:Client mongoClient = check new ({
    connection: string `mongodb://${MONGO_USER}:${MONGO_PASSWORD}@${MONGO_HOST}:${MONGO_PORT}/?authSource=admin`
});

isolated function notificationsCollection() returns mongodb:Collection|error {
    mongodb:Database db = check mongoClient->getDatabase(MONGO_DB);
    return check db->getCollection("notifications");
}

isolated function handledEventsCollection() returns mongodb:Collection|error {
    mongodb:Database db = check mongoClient->getDatabase(MONGO_DB);
    return check db->getCollection("handled_events");
}

public function saveNotification(Notification n) returns error? {
    mongodb:Collection c = check notificationsCollection();
    check c->insertOne(n);
}

public function listNotifications(string? orderId, string? recipientId) returns Notification[]|error {
    mongodb:Collection c = check notificationsCollection();
    map<json> filter = {};
    if orderId is string {
        filter["orderId"] = orderId;
    }
    if recipientId is string {
        filter["recipientId"] = recipientId;
    }
    Notification[] items = [];
    stream<Notification, error?>|error resultOrError = c->find(filter, targetType = Notification);
    stream<Notification, error?> result = check resultOrError;
    error? iterateError = from Notification n in result
        do {
            items.push(n);
        };
    if iterateError is error {
        return iterateError;
    }
    return items;
}

public function listNotificationsByOrder(string orderId) returns Notification[]|error {
    mongodb:Collection c = check notificationsCollection();
    Notification[] items = [];
    stream<Notification, error?>|error resultOrError = c->find({"orderId": orderId}, targetType = Notification);
    stream<Notification, error?> result = check resultOrError;
    error? iterateError = from Notification n in result
        do {
            items.push(n);
        };
    if iterateError is error {
        return iterateError;
    }
    return items;
}

// Returns true if the marker was newly inserted, false if the event
// has already been handled.
public function markEventHandled(string eventId, string notificationId) returns boolean|error {
    mongodb:Collection c = check handledEventsCollection();
    error? insertError = c->insertOne({
        "eventId": eventId,
        "notificationId": notificationId,
        "handledAt": nowUtc()
    });
    if insertError is error {
        if insertError.message().includes("E11000") {
            return false;
        }
        return insertError;
    }
    return true;
}
