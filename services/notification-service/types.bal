public const string RECIPIENT_CUSTOMER = "CUSTOMER";
public const string RECIPIENT_RESTAURANT = "RESTAURANT";
public const string RECIPIENT_DRIVER = "DRIVER";
public const string RECIPIENT_OPS = "OPS";

public const string CHANNEL_PUSH = "PUSH";
public const string CHANNEL_SMS = "SMS";
public const string CHANNEL_EMAIL = "EMAIL";

public type Notification record {|
    string notificationId;
    string orderId;
    string recipientType;
    string recipientId;
    string channel;
    string subject;
    string message;
    string sentAt;
|};

public type HandledEvent record {|
    string eventId;
    string notificationId;
    string handledAt;
|};

public type NotificationListResponse record {|
    Notification[] items;
    int total;
|};

public type ApiError record {|
    string code;
    string message;
|};
