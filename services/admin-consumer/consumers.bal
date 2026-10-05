// Orders group. Builds the initial order_facts rows from orders.created
// and updates them on orders.status.changed.

import ballerina/lang.value;
import ballerina/log;
import ballerinax/kafka;

public function startConsumers() returns error? {
    check ordersConsumer->subscribe([TOPIC_ORDERS_CREATED, TOPIC_ORDERS_STATUS_CHANGED]);
    log:printInfo("orders group subscribed");
    return ordersLoop();
}

function ordersLoop() returns error? {
    while true {
        kafka:AnydataConsumerRecord[] records = check ordersConsumer->poll(1.0);
        foreach kafka:AnydataConsumerRecord rec in records {
            do {
                check handleOrderRecord(rec);
            } on fail error e {
                log:printError("orders handler failed", orderId = extractOrderId(rec));
            }
        }
        error? commitError = ordersConsumer->'commit();
        if commitError is error {
            log:printError("commit failed", commitError);
        }
    }
}

function handleOrderRecord(kafka:AnydataConsumerRecord rec) returns error? {
    json payload = check decodeRecord(rec);
    OrderEvent event = check payload.cloneWithType(OrderEvent);

    OrderFact? existing = check findOrderFact(event.orderId);
    if existing is () {
        OrderFact fact = {
            orderId: event.orderId,
            customerId: event.customerId,
            restaurantId: event.restaurantId,
            restaurantName: "",
            status: event.status,
            totalAmount: event.totalAmount,
            createdAt: event.occurredAt,
            deliveredAt: (),
            cancelledAt: event.status == "CANCELLED" ? event.occurredAt : (),
            cancellationReason: event.status == "CANCELLED" ? event.reason : (),
            driverId: (),
            driverName: (),
            assignedAt: (),
            pickedUpAt: (),
            completedAt: ()
        };
        check saveOrderFact(fact);
        return;
    }

    OrderFact updated = existing;
    updated.status = event.status;
    if event.status == "DELIVERED" {
        updated.deliveredAt = event.occurredAt;
    } else if event.status == "CANCELLED" {
        updated.cancelledAt = event.occurredAt;
        updated.cancellationReason = event.reason;
    }
    check updateOrderFact(updated);
}

isolated function decodeRecord(kafka:AnydataConsumerRecord rec) returns json|error {
    byte[] raw = check rec.value.ensureType();
    string text = check string:fromBytes(raw);
    return value:fromJsonString(text);
}

isolated function extractOrderId(kafka:AnydataConsumerRecord rec) returns string {
    byte[]|error raw = rec.value.ensureType();
    if raw is error {
        return "unknown";
    }
    string|error text = string:fromBytes(raw);
    if text is error {
        return "unknown";
    }
    json|error parsed = value:fromJsonString(text);
    if parsed is error {
        return "unknown";
    }
    if parsed is map<json> {
        json v = parsed["orderId"] ?: "unknown";
        return v is string ? v : "unknown";
    }
    return "unknown";
}
