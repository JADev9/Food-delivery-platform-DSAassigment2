// Notification consumer. One group, subscribes to every event type.
// Each message becomes one or more notifications. The handled_events
// marker prevents a replayed event from sending a second alert.

import ballerina/lang.value;
import ballerina/log;
import ballerina/uuid;
import ballerinax/kafka;

public function startConsumers() returns error? {
    check notificationsConsumer->subscribe([
        TOPIC_ORDERS_CREATED,
        TOPIC_ORDERS_CONFIRMED,
        TOPIC_ORDERS_STATUS_CHANGED,
        TOPIC_ORDERS_CANCELLED,
        TOPIC_PAYMENTS_COMPLETED,
        TOPIC_PAYMENTS_FAILED,
        TOPIC_KITCHEN_ACCEPTED,
        TOPIC_KITCHEN_READY,
        TOPIC_DELIVERY_ASSIGNED,
        TOPIC_DELIVERY_PICKED_UP,
        TOPIC_DELIVERY_COMPLETED,
        TOPIC_DELIVERY_UNASSIGNED
    ]);
    log:printInfo("consumers started");
    return notificationsLoop();
}

function notificationsLoop() returns error? {
    while true {
        kafka:AnydataConsumerRecord[] records = check notificationsConsumer->poll(1.0);
        foreach kafka:AnydataConsumerRecord rec in records {
            do {
                check handleRecord(rec);
            } on fail error e {
                log:printError("notification handler failed", orderId = extractOrderId(rec));
            }
        }
        error? commitError = notificationsConsumer->'commit();
        if commitError is error {
            log:printError("commit failed", commitError);
        }
    }
}

function handleRecord(kafka:AnydataConsumerRecord rec) returns error? {
    json payload = check decodeRecord(rec);
    string eventId = check payload.eventId.ensureType(string);
    string eventType = check payload.eventType.ensureType(string);
    string orderId = check payload.orderId.ensureType(string);

    // Idempotency: has this event already produced a notification?
    string placeholderId = "pending";
    boolean fresh = check markEventHandled(eventId, placeholderId);
    if !fresh {
        return;
    }

    if eventType.startsWith("orders.") {
        OrderEvent event = check payload.cloneWithType(OrderEvent);
        return handleOrderEvent(event);
    }
    if eventType.startsWith("payments.") {
        PaymentEvent event = check payload.cloneWithType(PaymentEvent);
        return handlePaymentEvent(event);
    }
    if eventType.startsWith("restaurant.") {
        KitchenEvent event = check payload.cloneWithType(KitchenEvent);
        return handleKitchenEvent(event);
    }
    if eventType.startsWith("delivery.") {
        DeliveryEvent event = check payload.cloneWithType(DeliveryEvent);
        return handleDeliveryEvent(event);
    }
    return;
}

function handleOrderEvent(OrderEvent event) returns error? {
    if event.eventType == TOPIC_ORDERS_CREATED {
        check notify(event.orderId, RECIPIENT_CUSTOMER, event.customerId, CHANNEL_PUSH,
            "Order received",
            string `We have your order, total N$ ${event.totalAmount}.`);
        return;
    }
    if event.eventType == TOPIC_ORDERS_CONFIRMED {
        check notify(event.orderId, RECIPIENT_CUSTOMER, event.customerId, CHANNEL_PUSH,
            "Order confirmed",
            "Your order is confirmed and the kitchen has been notified.");
        check notify(event.orderId, RECIPIENT_RESTAURANT, event.restaurantId, CHANNEL_PUSH,
            "New order",
            string `New order ${event.orderId}, please confirm.`);
        return;
    }
    if event.eventType == TOPIC_ORDERS_STATUS_CHANGED {
        if event.status == "OUT_FOR_DELIVERY" {
            check notify(event.orderId, RECIPIENT_CUSTOMER, event.customerId, CHANNEL_PUSH,
                "On the way",
                "Your order is on the way.");
        } else if event.status == "DELIVERED" {
            check notify(event.orderId, RECIPIENT_CUSTOMER, event.customerId, CHANNEL_PUSH,
                "Delivered",
                "Your order has been delivered. Enjoy!");
        }
        return;
    }
    if event.eventType == TOPIC_ORDERS_CANCELLED {
        check notify(event.orderId, RECIPIENT_CUSTOMER, event.customerId, CHANNEL_PUSH,
            "Order cancelled",
            string `Your order was cancelled: ${event.reason ?: "no reason given"}.`);
        return;
    }
}

function handlePaymentEvent(PaymentEvent event) returns error? {
    if event.status == "COMPLETED" {
        check notify(event.orderId, RECIPIENT_CUSTOMER, event.customerId, CHANNEL_SMS,
            "Payment received",
            string `We charged N$ ${event.amount}.`);
    } else {
        check notify(event.orderId, RECIPIENT_CUSTOMER, event.customerId, CHANNEL_SMS,
            "Payment failed",
            string `Your payment could not be processed: ${event.failureReason ?: "unknown"}.`);
    }
}

function handleKitchenEvent(KitchenEvent event) returns error? {
    if event.status == "ACCEPTED" {
        check notify(event.orderId, RECIPIENT_CUSTOMER, event.orderId, CHANNEL_PUSH,
            "Cooking",
            string `Your food is being prepared. Ready in about ${event.prepTimeMinutes} minutes.`);
    } else if event.status == "READY" {
        check notify(event.orderId, RECIPIENT_OPS, "ops", CHANNEL_PUSH,
            "Ready for pickup",
            string `Order ${event.orderId} is ready to be collected.`);
    } else if event.status == "REJECTED" {
        check notify(event.orderId, RECIPIENT_CUSTOMER, event.orderId, CHANNEL_PUSH,
            "Order rejected",
            string `The kitchen could not accept your order: ${event.reason ?: "unknown"}.`);
    }
}

function handleDeliveryEvent(DeliveryEvent event) returns error? {
    if event.eventType == TOPIC_DELIVERY_ASSIGNED {
        check notify(event.orderId, RECIPIENT_CUSTOMER, event.customerId, CHANNEL_PUSH,
            "Driver on the way",
            string `${event.driverName} is picking up your order.`);
        if event.driverId != "" {
            check notify(event.orderId, RECIPIENT_DRIVER, event.driverId, CHANNEL_PUSH,
                "New delivery",
                string `Collect order ${event.orderId} from ${event.restaurantId}.`);
        }
    } else if event.eventType == TOPIC_DELIVERY_COMPLETED {
        check notify(event.orderId, RECIPIENT_RESTAURANT, event.restaurantId, CHANNEL_PUSH,
            "Delivery complete",
            string `Order ${event.orderId} was handed over.`);
    } else if event.eventType == TOPIC_DELIVERY_UNASSIGNED {
        check notify(event.orderId, RECIPIENT_OPS, "ops", CHANNEL_PUSH,
            "No driver available",
            string `Order ${event.orderId} could not be assigned: ${event.reason ?: "unknown"}.`);
    }
}

// Records a notification and publishes the audit event.
function notify(string orderId, string recipientType, string recipientId, string channel,
                string subject, string message) returns error? {
    string notificationId = uuid:createType4AsString();
    string now = nowUtc();

    Notification n = {
        notificationId: notificationId,
        orderId: orderId,
        recipientType: recipientType,
        recipientId: recipientId,
        channel: channel,
        subject: subject,
        message: message,
        sentAt: now
    };
    check saveNotification(n);
    log:printInfo("notification sent", orderId = orderId, recipientType = recipientType, subject = subject);
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
