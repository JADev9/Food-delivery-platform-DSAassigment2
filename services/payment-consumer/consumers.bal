import ballerina/lang.runtime;
// Payment consumer. One group, two topics.
//
// Charging happens only here, never through the REST API. The unique
// index on orderId in the payments collection means a replayed
// orders.created event cannot create a second charge.

import ballerina/lang.value;
import ballerina/log;
import ballerina/uuid;
import ballerinax/kafka;

final kafka:Consumer paymentsConsumer = check new (KAFKA_BOOTSTRAP, {
    groupId: "payment-service-orders",
    autoCommit: false,
    clientId: SERVICE_NAME + "-consumer"
});

public function startConsumers() returns error? {
    check paymentsConsumer->subscribe([TOPIC_ORDERS_CREATED, TOPIC_ORDERS_CANCELLED]);
    log:printInfo("consumers started");
    return paymentsLoop();
}

function paymentsLoop() returns error? {
    while true {
        kafka:AnydataConsumerRecord[] records = check paymentsConsumer->poll(1.0);
        foreach kafka:AnydataConsumerRecord rec in records {
            do {
                check handleRecord(rec);
            } on fail error e {
                log:printError("payment handler failed", orderId = extractOrderId(rec));
            }
        }
        error? commitError = paymentsConsumer->'commit();
        if commitError is error {
            log:printError("commit failed", commitError);
        }
    }
}

function handleRecord(kafka:AnydataConsumerRecord rec) returns error? {
    json payload = check decodeRecord(rec);
    string eventType = check payload.eventType.ensureType(string);

    if eventType == TOPIC_ORDERS_CREATED {
        OrderEvent event = check payload.cloneWithType(OrderEvent);
        return chargeOrder(event);
    }
    if eventType == TOPIC_ORDERS_CANCELLED {
        OrderEvent event = check payload.cloneWithType(OrderEvent);
        return refundOrder(event);
    }
    return;
}

function chargeOrder(OrderEvent event) returns error? {
    // Idempotency: if a payment already exists for this order, do nothing.
    Payment? existing = check findPaymentByOrder(event.orderId);
    if existing is Payment {
        log:printInfo("payment already exists", orderId = event.orderId);
        return;
    }

    // Simulated latency, so the demo shows a real processing delay.
    if PAYMENT_LATENCY_MS > 0 {
        runtime:sleep(<decimal>PAYMENT_LATENCY_MS / 1000d);
    }

    string paymentId = uuid:createType4AsString();
    string now = nowUtc();
    decimal amount = event.totalAmount;

    // Simulation: orders above the decline threshold are refused. This
    // is the repeatable way to demonstrate the compensating path.
    if amount > PAYMENT_DECLINE_ABOVE {
        Payment declined = {
            paymentId: paymentId,
            orderId: event.orderId,
            customerId: event.customerId,
            amount: amount,
            status: PAYMENT_FAILED,
            failureReason: string `amount ${amount} exceeds decline threshold ${PAYMENT_DECLINE_ABOVE}`,
            processedAt: now,
            refundedAt: (),
            refundAmount: ()
        };
        check savePayment(declined);

        PaymentEvent outgoing = {
            eventId: newEventId(),
            eventType: TOPIC_PAYMENTS_FAILED,
            occurredAt: now,
            paymentId: paymentId,
            orderId: event.orderId,
            customerId: event.customerId,
            amount: amount,
            status: PAYMENT_FAILED,
            failureReason: declined.failureReason
        };
        check publish(TOPIC_PAYMENTS_FAILED, outgoing, event.orderId);
        log:printInfo("payment declined", orderId = event.orderId, amount = amount);
        return;
    }

    // Charge succeeds.
    Payment payment = {
        paymentId: paymentId,
        orderId: event.orderId,
        customerId: event.customerId,
        amount: amount,
        status: PAYMENT_COMPLETED,
        failureReason: (),
        processedAt: now,
        refundedAt: (),
        refundAmount: ()
    };
    check savePayment(payment);

    PaymentEvent outgoing = {
        eventId: newEventId(),
        eventType: TOPIC_PAYMENTS_COMPLETED,
        occurredAt: now,
        paymentId: paymentId,
        orderId: event.orderId,
        customerId: event.customerId,
        amount: amount,
        status: PAYMENT_COMPLETED,
        failureReason: ()
    };
    check publish(TOPIC_PAYMENTS_COMPLETED, outgoing, event.orderId);
    log:printInfo("payment completed", orderId = event.orderId, amount = amount);
}

function refundOrder(OrderEvent event) returns error? {
    Payment? maybePayment = check findPaymentByOrder(event.orderId);
    if maybePayment is () {
        // Cancellation may arrive before payment for orders declined
        // upstream. Nothing to refund.
        return;
    }

    Payment payment = maybePayment;
    if payment.status == PAYMENT_REFUNDED || payment.status == PAYMENT_FAILED {
        // Already refunded, or nothing was charged in the first place.
        return;
    }

    string now = nowUtc();
    payment.status = PAYMENT_REFUNDED;
    payment.refundedAt = now;
    payment.refundAmount = payment.amount;
    check updatePayment(payment);

    log:printInfo("payment refunded", orderId = event.orderId, amount = payment.amount);
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
