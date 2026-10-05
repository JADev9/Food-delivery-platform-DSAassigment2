// Domain types for the order service. The state machine's transition
// table lives here too — every legal move between statuses is declared
// once, and every state change in the service is validated against it.

public const string CREATED = "CREATED";
public const string CONFIRMED = "CONFIRMED";
public const string PREPARING = "PREPARING";
public const string READY = "READY";
public const string OUT_FOR_DELIVERY = "OUT_FOR_DELIVERY";
public const string DELIVERED = "DELIVERED";
public const string CANCELLED = "CANCELLED";

public type OrderStatus CREATED|CONFIRMED|PREPARING|READY|OUT_FOR_DELIVERY|DELIVERED|CANCELLED;

// Menu item details are copied onto the order at placement time. A later
// menu edit must not rewrite an order that has already been placed.
public type OrderItem record {|
    string menuItemId;
    string name;
    int quantity;
    decimal unitPrice;
|};

// One entry per state change. origin is either "HTTP" or the name of
// the Kafka event that drove the transition.
public type AuditEntry record {|
    string at;
    OrderStatus status;
    string origin;
    string? actor;
|};

public type Order record {|
    string orderId;
    string customerId;
    string restaurantId;
    OrderItem[] items;
    decimal totalAmount;
    string deliveryAddress;
    OrderStatus status;
    string? reason;
    string createdAt;
    string updatedAt;
    AuditEntry[] history;
|};

// Every legal transition. Anything not listed here is rejected by
// transition() in lifecycle.bal.
public final map<OrderStatus[]> ALLOWED_TRANSITIONS = {
    "CREATED": [CONFIRMED, CANCELLED],
    "CONFIRMED": [PREPARING, CANCELLED],
    "PREPARING": [READY, CANCELLED],
    "READY": [OUT_FOR_DELIVERY, CANCELLED],
    "OUT_FOR_DELIVERY": [DELIVERED],
    "DELIVERED": [],
    "CANCELLED": []
};

// --- HTTP request/response shapes -------------------------------------

public type PlaceOrderItem record {|
    string menuItemId;
    string name;
    int quantity;
    decimal unitPrice;
|};

public type PlaceOrderRequest record {|
    string customerId;
    string restaurantId;
    string deliveryAddress;
    PlaceOrderItem[] items;
|};

public type CancelOrderRequest record {|
    string reason;
|};

public type OrderListResponse record {|
    Order[] items;
    int total;
|};

public type ApiError record {|
    string code;
    string message;
|};
