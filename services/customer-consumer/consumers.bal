// Customer consumer. One group. Records lifetime spend when an order
// is delivered.

import ballerina/lang.value;
import ballerina/log;
import ballerinax/kafka;

public function startConsumers() returns error? {
    check ordersConsumer->subscribe([TOPIC_ORDERS_STATUS_CHANGED]);
    log:printInfo("consumers started");
    return ordersLoop();
}

function ordersLoop() returns error? {
    while true {
        kafka:AnydataConsumerRecord[] records = check ordersConsumer->poll(1.0);
        foreach kafka:AnydataConsumerRecord rec in records {
            do {
                check handleOrderStatus(rec);
            } on fail error e {
                log:printError("customer handler failed", orderId = extractOrderId(rec));
            }
        }
        error? commitError = ordersConsumer->'commit();
        if commitError is error {
            log:printError("commit failed", commitError);
        }
    }
}

function handleOrderStatus(kafka:AnydataConsumerRecord rec) returns error? {
    json payload = check decodeRecord(rec);
    OrderEvent event = check payload.cloneWithType(OrderEvent);

    if event.status != "DELIVERED" {
        return;
    }

    Customer? maybeCustomer = check findCustomer(event.customerId);
    if maybeCustomer is () {
        log:printWarn("delivered order for unknown customer", customerId = event.customerId);
        return;
    }

    Customer customer = maybeCustomer;
    customer.lifetimeOrders += 1;
    customer.lifetimeSpend += event.totalAmount;
    customer.updatedAt = nowUtc();
    check updateCustomer(customer);
    log:printInfo("lifetime spend updated", customerId = event.customerId, orderId = event.orderId);
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
