public const string PAYMENT_COMPLETED = "COMPLETED";
public const string PAYMENT_FAILED = "FAILED";
public const string PAYMENT_REFUNDED = "REFUNDED";

public type Payment record {|
    string paymentId;
    string orderId;
    string customerId;
    decimal amount;
    string status;
    string? failureReason;
    string processedAt;
    string? refundedAt;
    decimal? refundAmount;
|};

public type PaymentListResponse record {|
    Payment[] items;
    int total;
|};

public type ApiError record {|
    string code;
    string message;
|};
