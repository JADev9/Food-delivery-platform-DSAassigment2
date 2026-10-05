// Read-only payment endpoints. Charging happens only in the consumer,
// in response to orders.created, so there is exactly one code path that
// can bill a customer.

import ballerina/http;
import ballerina/log;

service /api/v1 on new http:Listener(SERVICE_PORT) {

    resource function get health() returns json {
        return {status: "UP", 'service: SERVICE_NAME};
    }

    resource function get payments(string? customerId) returns PaymentListResponse|http:InternalServerError {
        Payment[]|error found = listPayments(customerId);
        if found is error {
            log:printError("failed to list payments", found);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not list payments"}
            };
        }
        return {items: found, total: found.length()};
    }

    resource function get payments/[string paymentId]() returns Payment|http:NotFound|http:InternalServerError {
        Payment?|error found = findPayment(paymentId);
        if found is error {
            log:printError("failed to find payment", found, paymentId = paymentId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not find payment"}
            };
        }
        if found is () {
            return <http:NotFound>{
                body: {code: "NOT_FOUND", message: string `no payment ${paymentId}`}
            };
        }
        return found;
    }

    resource function get orders/[string orderId]/payment() returns Payment|http:NotFound|http:InternalServerError {
        Payment?|error found = findPaymentByOrder(orderId);
        if found is error {
            log:printError("failed to find payment", found, orderId = orderId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not find payment"}
            };
        }
        if found is () {
            return <http:NotFound>{
                body: {code: "NOT_FOUND", message: string `no payment for order ${orderId}`}
            };
        }
        return found;
    }
}
