import ballerina/time;
import ballerina/uuid;

public const string TOPIC_ORDERS_CREATED = "orders.created";
public const string TOPIC_ORDERS_CONFIRMED = "orders.confirmed";
public const string TOPIC_ORDERS_STATUS_CHANGED = "orders.status.changed";
public const string TOPIC_ORDERS_CANCELLED = "orders.cancelled";
public const string TOPIC_PAYMENTS_COMPLETED = "payments.completed";
public const string TOPIC_PAYMENTS_FAILED = "payments.failed";
public const string TOPIC_KITCHEN_ACCEPTED = "restaurant.order.accepted";
public const string TOPIC_KITCHEN_READY = "restaurant.order.ready";
public const string TOPIC_DELIVERY_ASSIGNED = "delivery.assigned";
public const string TOPIC_DELIVERY_PICKED_UP = "delivery.picked-up";
public const string TOPIC_DELIVERY_COMPLETED = "delivery.completed";
public const string TOPIC_DELIVERY_UNASSIGNED = "delivery.unassigned";
public const string TOPIC_NOTIFICATIONS_DISPATCHED = "notifications.dispatched";

public type OrderItem record {|
    string menuItemId;
    string name;
    int quantity;
    decimal unitPrice;
|};

public type OrderEvent record {|
    string eventId;
    string eventType;
    string occurredAt;
    string orderId;
    string customerId;
    string restaurantId;
    OrderItem[] items;
    decimal totalAmount;
    string status;
    string deliveryAddress;
    string? reason;
|};

public type PaymentEvent record {|
    string eventId;
    string eventType;
    string occurredAt;
    string paymentId;
    string orderId;
    string customerId;
    decimal amount;
    string status;
    string? failureReason;
|};

public type KitchenEvent record {|
    string eventId;
    string eventType;
    string occurredAt;
    string orderId;
    string restaurantId;
    string status;
    int prepTimeMinutes;
    string? reason;
|};

public type DeliveryEvent record {|
    string eventId;
    string eventType;
    string occurredAt;
    string deliveryId;
    string orderId;
    string restaurantId;
    string customerId;
    string driverId;
    string driverName;
    string status;
    string? reason;
|};

public isolated function newEventId() returns string => uuid:createType4AsString();

public isolated function nowUtc() returns string => time:utcToString(time:utcNow());
