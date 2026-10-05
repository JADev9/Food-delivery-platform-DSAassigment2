import ballerinax/mongodb;

final mongodb:Client mongoClient = check new ({
    connection: string `mongodb://${MONGO_USER}:${MONGO_PASSWORD}@${MONGO_HOST}:${MONGO_PORT}/?authSource=admin`
});

isolated function customersCollection() returns mongodb:Collection|error {
    mongodb:Database db = check mongoClient->getDatabase(MONGO_DB);
    return check db->getCollection("customers");
}

isolated function addressesCollection() returns mongodb:Collection|error {
    mongodb:Database db = check mongoClient->getDatabase(MONGO_DB);
    return check db->getCollection("addresses");
}

public function saveCustomer(Customer c) returns error? {
    mongodb:Collection col = check customersCollection();
    check col->insertOne(c);
}

public function updateCustomer(Customer c) returns error? {
    mongodb:Collection col = check customersCollection();
    mongodb:UpdateResult _ = check col->updateOne({"customerId": c.customerId}, {set: c});
}

public function findCustomer(string customerId) returns Customer?|error {
    mongodb:Collection col = check customersCollection();
    stream<Customer, error?>|error resultOrError = col->find({"customerId": customerId}, targetType = Customer);
    stream<Customer, error?> result = check resultOrError;
    Customer? found = ();
    error? iterateError = from Customer c in result
        do {
            found = c;
        };
    if iterateError is error {
        return iterateError;
    }
    return found;
}

public function findCustomerByEmail(string email) returns Customer?|error {
    mongodb:Collection col = check customersCollection();
    stream<Customer, error?>|error resultOrError = col->find({"email": email}, targetType = Customer);
    stream<Customer, error?> result = check resultOrError;
    Customer? found = ();
    error? iterateError = from Customer c in result
        do {
            found = c;
        };
    if iterateError is error {
        return iterateError;
    }
    return found;
}

public function listCustomers() returns Customer[]|error {
    mongodb:Collection col = check customersCollection();
    Customer[] items = [];
    stream<Customer, error?>|error resultOrError = col->find({}, targetType = Customer);
    stream<Customer, error?> result = check resultOrError;
    error? iterateError = from Customer c in result
        do {
            items.push(c);
        };
    if iterateError is error {
        return iterateError;
    }
    return items;
}

public function saveAddress(Address a) returns error? {
    mongodb:Collection col = check addressesCollection();
    check col->insertOne(a);
}

public function listAddresses(string customerId) returns Address[]|error {
    mongodb:Collection col = check addressesCollection();
    Address[] items = [];
    stream<Address, error?>|error resultOrError = col->find({"customerId": customerId}, targetType = Address);
    stream<Address, error?> result = check resultOrError;
    error? iterateError = from Address a in result
        do {
            items.push(a);
        };
    if iterateError is error {
        return iterateError;
    }
    return items;
}

public function clearDefaultAddress(string customerId) returns error? {
    mongodb:Collection col = check addressesCollection();
    mongodb:UpdateResult _ = check col->updateMany({"customerId": customerId, "isDefault": true}, {set: {"isDefault": false}});
}
