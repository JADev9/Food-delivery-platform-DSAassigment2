// Kitchen group. Records the restaurant name on the order_facts row so
// reports can show it without a second lookup.

import ballerina/lang.value;
import ballerina/log;
import ballerinax/kafka;

public function startConsumers() returns error? {
    check kitchenConsumer->subscribe([TOPIC_KITCHEN_ACCEPTED]);
    log:printInfo("kitchen group subscribed");
    return kitchenLoop();
}

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
        error? commitError = kitchenConsumer->'commit();
        if commitError is error {
            log:printError("commit failed", commitError);
        }
    }
}

function handleKitchenRecord(kafka:AnydataConsumerRecord rec) returns error? {
    json payload = check decodeRecord(rec);
    KitchenEvent event = check payload.cloneWithType(KitchenEvent);

    OrderFact? existing = check findOrderFact(event.orderId);
    if existing is () {
        return;
    }

    OrderFact updated = existing;
    updated.restaurantId = event.restaurantId;
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
