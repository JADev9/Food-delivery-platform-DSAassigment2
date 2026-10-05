// Delivery group. Records driver assignment and completion on the
// order_facts row.

import ballerina/lang.value;
import ballerina/log;
import ballerinax/kafka;

public function startConsumers() returns error? {
    check deliveryConsumer->subscribe([TOPIC_DELIVERY_ASSIGNED, TOPIC_DELIVERY_COMPLETED]);
    log:printInfo("delivery group subscribed");
    return deliveryLoop();
}

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
        error? commitError = deliveryConsumer->'commit();
        if commitError is error {
            log:printError("commit failed", commitError);
        }
    }
}

function handleDeliveryRecord(kafka:AnydataConsumerRecord rec) returns error? {
    json payload = check decodeRecord(rec);
    DeliveryEvent event = check payload.cloneWithType(DeliveryEvent);

    OrderFact? existing = check findOrderFact(event.orderId);
    if existing is () {
        return;
    }

    OrderFact updated = existing;
    if event.eventType == TOPIC_DELIVERY_ASSIGNED {
        updated.driverId = event.driverId;
        updated.driverName = event.driverName;
        updated.assignedAt = event.occurredAt;
    } else if event.eventType == TOPIC_DELIVERY_COMPLETED {
        updated.completedAt = event.occurredAt;
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
