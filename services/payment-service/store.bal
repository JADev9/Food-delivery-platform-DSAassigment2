import ballerinax/mongodb;

final mongodb:Client mongoClient = check new ({
    connection: string `mongodb://${MONGO_USER}:${MONGO_PASSWORD}@${MONGO_HOST}:${MONGO_PORT}/?authSource=admin`
});

isolated function paymentsCollection() returns mongodb:Collection|error {
    mongodb:Database db = check mongoClient->getDatabase(MONGO_DB);
    return check db->getCollection("payments");
}

public function savePayment(Payment p) returns error? {
    mongodb:Collection c = check paymentsCollection();
    check c->insertOne(p);
}

public function updatePayment(Payment p) returns error? {
    mongodb:Collection c = check paymentsCollection();
    mongodb:UpdateResult _ = check c->updateOne({"paymentId": p.paymentId}, {set: p});
}

public function findPayment(string paymentId) returns Payment?|error {
    mongodb:Collection c = check paymentsCollection();
    stream<Payment, error?>|error resultOrError = c->find({"paymentId": paymentId}, targetType = Payment);
    stream<Payment, error?> result = check resultOrError;
    Payment? found = ();
    error? iterateError = from Payment p in result
        do {
            found = p;
        };
    if iterateError is error {
        return iterateError;
    }
    return found;
}

public function findPaymentByOrder(string orderId) returns Payment?|error {
    mongodb:Collection c = check paymentsCollection();
    stream<Payment, error?>|error resultOrError = c->find({"orderId": orderId}, targetType = Payment);
    stream<Payment, error?> result = check resultOrError;
    Payment? found = ();
    error? iterateError = from Payment p in result
        do {
            found = p;
        };
    if iterateError is error {
        return iterateError;
    }
    return found;
}

public function listPayments(string? customerId) returns Payment[]|error {
    mongodb:Collection c = check paymentsCollection();
    map<json> filter = {};
    if customerId is string {
        filter["customerId"] = customerId;
    }
    Payment[] payments = [];
    stream<Payment, error?>|error resultOrError = c->find(filter, targetType = Payment);
    stream<Payment, error?> result = check resultOrError;
    error? iterateError = from Payment p in result
        do {
            payments.push(p);
        };
    if iterateError is error {
        return iterateError;
    }
    return payments;
}
