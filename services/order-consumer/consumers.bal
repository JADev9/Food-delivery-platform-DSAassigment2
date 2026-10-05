// Kafka consumers for order-service.
//
// Three consumer groups, one concern each. Separate groups mean a slow
// kitchen cannot block payment events, and each group commits its own
// offsets independently.
//
// Every handler calls transition() so no state change can bypass the
// transition table. A duplicate or illegal transition is logged and the
// offset is still committed — an event that cannot be applied is not
// worth replaying forever.

import ballerina/lang.value;
import ballerina/log;
import ballerinax/kafka;

final kafka:Consumer paymentsConsumer = check new (KAFKA_BOOTSTRAP, {
    groupId: "order-service-payments",
    autoCommit: false,
    clientId: SERVICE_NAME + "-payments"
});

final kafka:Consumer kitchenConsumer = check new (KAFKA_BOOTSTRAP, {
    groupId: "order-service-kitchen",
    autoCommit: false,
    clientId: SERVICE_NAME + "-kitchen"
});

final kafka:Consumer deliveryConsumer = check new (KAFKA_BOOTSTRAP, {
    groupId: "order-service-delivery",
    autoCommit: false,
    clientId: SERVICE_NAME + "-delivery"
});

public function startConsumers() returns error? {
    check paymentsConsumer->subscribe([TOPIC_PAYMENTS_COMPLETED, TOPIC_PAYMENTS_FAILED]);
    check kitchenConsumer->subscribe([TOPIC_KITCHEN_ACCEPTED, TOPIC_KITCHEN_READY]);
    check deliveryConsumer->subscribe([TOPIC_DELIVERY_PICKED_UP, TOPIC_DELIVERY_COMPLETED]);

    _ = start paymentsLoop();
    _ = start kitchenLoop();
    _ = start deliveryLoop();

    log:printInfo("consumers started");
}

// Decodes a Kafka record's value into a json document.
isolated function decodeRecord(kafka:AnydataConsumerRecord rec) returns json|error {
    byte[] raw = check rec.value.ensureType();
    string text = check string:fromBytes(raw);
    return value:fromJsonString(text);
}

// --- payments group --------------------------------------------------

function paymentsLoop() returns error? {
    while true {
        kafka:AnydataConsumerRecord[] records = check paymentsConsumer->poll(1.0);
        foreach kafka:AnydataConsumerRecord rec in records {
            do {
                check handlePaymentRecord(rec);
            } on fail error e {
                log:printError("payment handler failed", orderId = extractOrderId(rec));
            }
        }
        check paymentsConsumer->'commit();
    }
}

function handlePaymentRecord(kafka:AnydataConsumerRecord rec) returns error? {
    json payload = check decodeRecord(rec);
    PaymentEvent event = check payload.cloneWithType(PaymentEvent);

    Order? maybeOrder = check findOrder(event.orderId);
    if maybeOrder is () {
        log:printWarn("payment for unknown order, skipping", orderId = event.orderId);
        return;
    }
    Order existing = maybeOrder;

    OrderStatus target = event.status == "COMPLETED" ? CONFIRMED : CANCELLED;
    string? cancelReason = event.status == "COMPLETED" ? () : event.failureReason;

    Order|error result = transition(existing, target, event.eventType, ());
    if result is error {
        log:printWarn("transition rejected", orderId = event.orderId, detail = result.message());
        return;
    }
    Order updated = result;
    if cancelReason is string {
        updated.reason = cancelReason;
    }
    check updateOrder(updated);

    OrderEvent outgoing = orderToEvent(updated, event.eventType);
    if target == CONFIRMED {
        check publish(TOPIC_ORDERS_CONFIRMED, outgoing, updated.orderId);
    } else {
        check publish(TOPIC_ORDERS_CANCELLED, outgoing, updated.orderId);
    }
    check publish(TOPIC_ORDERS_STATUS_CHANGED, outgoing, updated.orderId);
}

// --- kitchen group ---------------------------------------------------

function kitchenLoop() returns error? {
    while true {
        kafka:AnydataConsumerRecord[] records = check kitchenConsumer->poll(1.0);
        foreach kafka:AnydataConsumerRecord rec in records {
            do {
                check handleKitchenRecord(rec);
            } on fail error e {
                log:printError("kitchen handler failed", orderId = extractOrderId(rec));
            }
        }
        check kitchenConsumer->'commit();
    }
}

function handleKitchenRecord(kafka:AnydataConsumerRecord rec) returns error? {
    json payload = check decodeRecord(rec);
    KitchenEvent event = check payload.cloneWithType(KitchenEvent);

    Order? maybeOrder = check findOrder(event.orderId);
    if maybeOrder is () {
        log:printWarn("kitchen event for unknown order, skipping", orderId = event.orderId);
        return;
    }
    Order existing = maybeOrder;

    OrderStatus target;
    if event.status == "ACCEPTED" {
        target = PREPARING;
    } else if event.status == "READY" {
        target = READY;
    } else {
        target = CANCELLED;
    }

    Order|error result = transition(existing, target, event.eventType, ());
    if result is error {
        log:printWarn("transition rejected", orderId = event.orderId, detail = result.message());
        return;
    }
    Order updated = result;
    if event.status == "REJECTED" && event.reason is string {
        updated.reason = event.reason;
    }
    check updateOrder(updated);

    OrderEvent outgoing = orderToEvent(updated, event.eventType);
    check publish(TOPIC_ORDERS_STATUS_CHANGED, outgoing, updated.orderId);
    if target == CANCELLED {
        check publish(TOPIC_ORDERS_CANCELLED, outgoing, updated.orderId);
    }
}

// --- delivery group --------------------------------------------------

function deliveryLoop() returns error? {
    while true {
        kafka:AnydataConsumerRecord[] records = check deliveryConsumer->poll(1.0);
        foreach kafka:AnydataConsumerRecord rec in records {
            do {
                check handleDeliveryRecord(rec);
            } on fail error e {
                log:printError("delivery handler failed", orderId = extractOrderId(rec));
            }
        }
        check deliveryConsumer->'commit();
    }
}

function handleDeliveryRecord(kafka:AnydataConsumerRecord rec) returns error? {
    json payload = check decodeRecord(rec);
    DeliveryEvent event = check payload.cloneWithType(DeliveryEvent);

    Order? maybeOrder = check findOrder(event.orderId);
    if maybeOrder is () {
        log:printWarn("delivery event for unknown order, skipping", orderId = event.orderId);
        return;
    }
    Order existing = maybeOrder;

    OrderStatus target;
    if event.eventType == TOPIC_DELIVERY_PICKED_UP {
        target = OUT_FOR_DELIVERY;
    } else if event.eventType == TOPIC_DELIVERY_COMPLETED {
        target = DELIVERED;
    } else {
        return;
    }

    Order|error result = transition(existing, target, event.eventType, event.driverId);
    if result is error {
        log:printWarn("transition rejected", orderId = event.orderId, detail = result.message());
        return;
    }
    Order updated = result;
    check updateOrder(updated);

    OrderEvent outgoing = orderToEvent(updated, event.eventType);
    check publish(TOPIC_ORDERS_STATUS_CHANGED, outgoing, updated.orderId);
}

// --- helpers ---------------------------------------------------------

isolated function orderToEvent(Order o, string eventType) returns OrderEvent {
    return {
        eventId: newEventId(),
        eventType: eventType,
        occurredAt: nowUtc(),
        orderId: o.orderId,
        customerId: o.customerId,
        restaurantId: o.restaurantId,
        items: o.items,
        totalAmount: o.totalAmount,
        status: o.status,
        deliveryAddress: o.deliveryAddress,
        reason: o.reason
    };
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
