// Domain types for delivery-service.
//
// Drivers exist to serve deliveries. A delivery is created when the
// kitchen marks an order ready, and it moves through exactly one path:
// ASSIGNED -> PICKED_UP -> COMPLETED, or UNASSIGNED if no driver
// can be found.

public const string DRIVER_AVAILABLE = "AVAILABLE";
public const string DRIVER_ON_DELIVERY = "ON_DELIVERY";
public const string DRIVER_OFF_SHIFT = "OFF_SHIFT";

public const string DELIVERY_ASSIGNED = "ASSIGNED";
public const string DELIVERY_PICKED_UP = "PICKED_UP";
public const string DELIVERY_COMPLETED = "COMPLETED";
public const string DELIVERY_UNASSIGNED = "UNASSIGNED";

public type GeoPoint record {|
    decimal latitude;
    decimal longitude;
|};

public type Driver record {|
    string driverId;
    string name;
    string phone;
    string vehicle;
    GeoPoint location;
    string locationUpdatedAt;
    string status;
    string? currentDeliveryId;
    string createdAt;
|};

public type Delivery record {|
    string deliveryId;
    string orderId;
    string restaurantId;
    string customerId;
    string driverId;
    string driverName;
    string status;
    decimal distanceKm;
    string? reason;
    string assignedAt;
    string? pickedUpAt;
    string? completedAt;
|};

// --- HTTP request/response shapes -------------------------------------

public type RegisterDriverRequest record {|
    string name;
    string phone;
    string vehicle;
    GeoPoint location;
|};

public type UpdateLocationRequest record {|
    decimal latitude;
    decimal longitude;
|};

public type UpdateStatusRequest record {|
    string status;
|};

public type DriverListResponse record {|
    Driver[] items;
    int total;
|};

public type DeliveryListResponse record {|
    Delivery[] items;
    int total;
|};

public type ApiError record {|
    string code;
    string message;
|};
