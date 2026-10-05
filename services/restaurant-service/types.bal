public type MenuItem record {|
    string menuItemId;
    string name;
    string category;
    decimal price;
    int stock;
|};

public type OpeningHours record {|
    string opensAt;
    string closesAt;
    string[] closedOn;
|};

public type Restaurant record {|
    string restaurantId;
    string name;
    string address;
    string phone;
    string[] cuisines;
    GeoPoint location;
    OpeningHours hours;
    boolean accepting;
    MenuItem[] menu;
    string createdAt;
|};

public type GeoPoint record {|
    decimal latitude;
    decimal longitude;
|};

public const string TICKET_ACCEPTED = "ACCEPTED";
public const string TICKET_READY = "READY";
public const string TICKET_REJECTED = "REJECTED";

public type TicketItem record {|
    string menuItemId;
    string name;
    int quantity;
|};

public type Ticket record {|
    string orderId;
    string restaurantId;
    TicketItem[] items;
    string status;
    int prepTimeMinutes;
    string receivedAt;
    string? readyAt;
|};

public type Reservation record {|
    string orderId;
    TicketItem[] itemsReserved;
    string reservedAt;
    boolean restored;
|};

// --- HTTP request/response shapes -------------------------------------

public type NewMenuItem record {|
    string name;
    string category;
    decimal price;
    int stock;
|};

public type RegisterRestaurantRequest record {|
    string name;
    string address;
    string phone;
    string[] cuisines;
    GeoPoint location;
    OpeningHours hours;
    NewMenuItem[] menu;
|};

public type AddMenuItemRequest record {|
    string name;
    string category;
    decimal price;
    int stock;
|};

public type SetStockRequest record {|
    int stock;
|};

public type RestaurantListResponse record {|
    Restaurant[] items;
    int total;
|};

public type MenuListResponse record {|
    MenuItem[] items;
    int total;
|};

public type TicketListResponse record {|
    Ticket[] items;
    int total;
|};

public type ApiError record {|
    string code;
    string message;
|};
