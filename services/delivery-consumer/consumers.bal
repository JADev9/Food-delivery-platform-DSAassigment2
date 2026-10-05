// Delivery-service consumer. One group: reacts to the kitchen marking
// an order ready by finding a driver and publishing the assignment.

import ballerina/lang.value;
import ballerina/log;
import ballerina/uuid;
import ballerinax/kafka;

final kafka:Consumer kitchenConsumer = check new (KAFKA_BOOTSTRAP, {
    groupId: "delivery-service-kitchen",
    autoCommit: false,
    clientId: SERVICE_NAME + "-kitchen"
});

public function startConsumers() returns error? {
    check kitchenConsumer->subscribe([TOPIC_KITCHEN_READY]);
    _ = start kitchenLoop();
    log:printInfo("consumers started");
}

function kitchenLoop() returns error? {
    while true {
        kafka:AnydataConsumerRecord[] records = check kitchenConsumer->poll(1.0);
        foreach kafka:AnydataConsumerRecord rec in records {
            do {
                check handleKitchenReady(rec);
            } on fail error e {
                log:printError("kitchen handler failed", orderId = extractOrderId(rec));
            }
        }
        check kitchenConsumer->'commit();
    }
}

function handleKitchenReady(kafka:AnydataConsumerRecord rec) returns error? {
    byte[] raw = check rec.value.ensureType();
    string text = check string:fromBytes(raw);
    json payload = check value:fromJsonString(text);
    KitchenEvent event = check payload.cloneWithType(KitchenEvent);

    // Only react to READY. ACCEPTED and REJECTED are the order-service's
    // concern; delivery-service only cares that the food is now waiting.
    if event.status != "READY" {
        return;
    }

    // If a delivery record already exists for this order, a previous
    // attempt got this far. Bail out so a replay does not double-assign.
    Delivery? existing = check findDeliveryByOrder(event.orderId);
    if existing is Delivery {
        log:printInfo("delivery already exists for order", orderId = event.orderId);
        return;
    }

    GeoPoint pickup = restaurantLocation(event.restaurantId);
    Delivery|error assigned = tryAssign(event, pickup);
    if assigned is error {
        log:printWarn("no driver available", orderId = event.orderId, reason = assigned.message());
        return;
    }
    log:printInfo("delivery assigned", orderId = event.orderId, driverId = assigned.driverId);
}

function tryAssign(KitchenEvent event, GeoPoint pickup) returns Delivery|error {
    Driver|error chosen = findNearestDriver(pickup);
    if chosen is error {
        // No driver available. Publish the unassigned event so the ops
        // team sees it and the customer gets an honest notification.
        DeliveryEvent unassigned = {
            eventId: newEventId(),
            eventType: TOPIC_DELIVERY_UNASSIGNED,
            occurredAt: nowUtc(),
            deliveryId: "",
            orderId: event.orderId,
            restaurantId: event.restaurantId,
            customerId: "",
            driverId: "",
            driverName: "",
            status: "UNASSIGNED",
            reason: chosen.message()
        };
        check publish(TOPIC_DELIVERY_UNASSIGNED, unassigned, event.orderId);
        return chosen;
    }

    Driver driver = chosen;
    string deliveryId = uuid:createType4AsString();
    string assignedAt = nowUtc();

    Delivery d = {
        deliveryId: deliveryId,
        orderId: event.orderId,
        restaurantId: event.restaurantId,
        customerId: "",
        driverId: driver.driverId,
        driverName: driver.name,
        status: DELIVERY_ASSIGNED,
        distanceKm: distanceKm(driver.location, pickup),
        reason: (),
        assignedAt: assignedAt,
        pickedUpAt: (),
        completedAt: ()
    };
    check saveDelivery(d);

    // Mark the driver unavailable so a second order cannot pick them.
    Driver updatedDriver = driver;
    updatedDriver.status = DRIVER_ON_DELIVERY;
    updatedDriver.currentDeliveryId = deliveryId;
    check updateDriver(updatedDriver);

    DeliveryEvent outgoing = {
        eventId: newEventId(),
        eventType: TOPIC_DELIVERY_ASSIGNED,
        occurredAt: assignedAt,
        deliveryId: deliveryId,
        orderId: event.orderId,
        restaurantId: event.restaurantId,
        customerId: "",
        driverId: driver.driverId,
        driverName: driver.name,
        status: "ASSIGNED",
        reason: ()
    };
    check publish(TOPIC_DELIVERY_ASSIGNED, outgoing, event.orderId);

    return d;
}

function extractOrderId(kafka:AnydataConsumerRecord rec) returns string {
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
