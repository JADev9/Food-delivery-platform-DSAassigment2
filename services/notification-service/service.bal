// Notification service REST API. Read-only: notifications are produced
// by the consumer, not through HTTP.

import ballerina/http;
import ballerina/log;

service /api/v1 on new http:Listener(SERVICE_PORT) {

    resource function get health() returns json {
        return {status: "UP", 'service: SERVICE_NAME};
    }

    resource function get notifications(string? orderId, string? recipientId) returns NotificationListResponse|http:InternalServerError {
        Notification[]|error found = listNotifications(orderId, recipientId);
        if found is error {
            log:printError("failed to list notifications", found);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not list notifications"}
            };
        }
        return {items: found, total: found.length()};
    }

    resource function get notifications/find/[string orderId]() returns NotificationListResponse|http:InternalServerError {
        Notification[]|error found = listNotificationsByOrder(orderId);
        if found is error {
            log:printError("failed to list notifications", found, orderId = orderId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not list notifications"}
            };
        }
        return {items: found, total: found.length()};
    }
}
