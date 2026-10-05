public type OrderFact record {|
    string orderId;
    string customerId;
    string restaurantId;
    string restaurantName;
    string status;
    decimal totalAmount;
    string createdAt;
    string? deliveredAt;
    string? cancelledAt;
    string? cancellationReason;
    string? driverId;
    string? driverName;
    string? assignedAt;
    string? pickedUpAt;
    string? completedAt;
|};

public type SummaryReport record {|
    int totalOrders;
    int delivered;
    int cancelled;
    int inProgress;
    decimal grossRevenue;
    decimal averageOrderValue;
    string generatedAt;
|};

public type RestaurantReport record {|
    string restaurantId;
    string restaurantName;
    int totalOrders;
    int delivered;
    int cancelled;
    decimal revenue;
|};

public type DriverReport record {|
    string driverId;
    string driverName;
    int totalDeliveries;
    decimal averageDeliveryMinutes;
|};

public type DeliveryPerformanceReport record {|
    decimal averageOrderToDeliveryMinutes;
    decimal averageReadyToDeliveryMinutes;
    int sampleSize;
    string generatedAt;
|};

public type OrderListResponse record {|
    OrderFact[] items;
    int total;
|};

public type ApiError record {|
    string code;
    string message;
|};
