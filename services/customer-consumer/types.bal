public type Customer record {|
    string customerId;
    string name;
    string email;
    string phone;
    int lifetimeOrders;
    decimal lifetimeSpend;
    string createdAt;
    string updatedAt;
|};

public type Address record {|
    string addressId;
    string customerId;
    string label;
    string line1;
    string suburb;
    string city;
    string instructions;
    boolean isDefault;
    string createdAt;
|};

public type RegisterCustomerRequest record {|
    string name;
    string email;
    string phone;
|};

public type SaveAddressRequest record {|
    string label;
    string line1;
    string suburb;
    string city;
    string instructions;
    boolean isDefault;
|};

public type CustomerListResponse record {|
    Customer[] items;
    int total;
|};

public type AddressListResponse record {|
    Address[] items;
    int total;
|};

public type ApiError record {|
    string code;
    string message;
|};
