import ballerina/log;
import ballerinax/mongodb;

final mongodb:Client mongoClient = check new ({
    connection: {
        serverAddress: {
            host: MONGO_HOST,
            port: MONGO_PORT
        },
        auth: <mongodb:ScramSha256AuthCredential>{
            username: MONGO_USER,
            password: MONGO_PASSWORD,
            database: "admin"
        }
    }
});

isolated function ordersCollection() returns mongodb:Collection|error {
    mongodb:Database db = check mongoClient->getDatabase(MONGO_DB);
    return check db->getCollection("orders");
}

isolated function eventsCollection() returns mongodb:Collection|error {
    mongodb:Database db = check mongoClient->getDatabase(MONGO_DB);
    return check db->getCollection("handled_events");
}

public function saveOrder(Order o) returns error? {
    mongodb:Collection c = check ordersCollection();
    check c->insertOne(o);
    log:printDebug("order saved", orderId = o.orderId);
}

public function updateOrder(Order o) returns error? {
    mongodb:Collection c = check ordersCollection();
    mongodb:UpdateResult _ = check c->updateOne({"orderId": o.orderId}, {set: o});
    log:printDebug("order updated", orderId = o.orderId, status = o.status);
}

public function findOrder(string orderId) returns Order?|error {
    mongodb:Collection c = check ordersCollection();
    stream<Order, error?>|error resultOrError = c->find({"orderId": orderId}, targetType = Order);
    stream<Order, error?> result = check resultOrError;
    Order? found = ();
    error? iterateError = from Order o in result
        do {
            found = o;
        };
    if iterateError is error {
        return iterateError;
    }
    return found;
}

public function listOrders(string? customerId, string? status) returns Order[]|error {
    mongodb:Collection c = check ordersCollection();
    map<json> filter = {};
    if customerId is string {
        filter["customerId"] = customerId;
    }
    if status is string {
        filter["status"] = status;
    }
    Order[] orders = [];
    stream<Order, error?>|error resultOrError = c->find(filter, targetType = Order);
    stream<Order, error?> result = check resultOrError;
    error? iterateError = from Order o in result
        do {
            orders.push(o);
        };
    if iterateError is error {
        return iterateError;
    }
    return orders;
}

public function markEventHandled(string eventId, string orderId) returns boolean|error {
    mongodb:Collection c = check eventsCollection();
    error? insertError = c->insertOne({
        "eventId": eventId,
        "orderId": orderId,
        "handledAt": nowUtc()
    });
    if insertError is error {
        // Duplicate-key: this event was already handled. Not an error.
        if insertError.message().includes("E11000") {
            return false;
        }
        // Anything else (connection lost, auth failed) must propagate so
        // the consumer retries instead of silently dropping the event.
        return insertError;
    }
    return true;
}
