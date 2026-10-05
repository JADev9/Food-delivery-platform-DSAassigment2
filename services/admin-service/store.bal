import ballerinax/mongodb;

final mongodb:Client mongoClient = check new ({
    connection: string `mongodb://${MONGO_USER}:${MONGO_PASSWORD}@${MONGO_HOST}:${MONGO_PORT}/?authSource=admin`
});

isolated function orderFactsCollection() returns mongodb:Collection|error {
    mongodb:Database db = check mongoClient->getDatabase(MONGO_DB);
    return check db->getCollection("order_facts");
}

public function saveOrderFact(OrderFact f) returns error? {
    mongodb:Collection c = check orderFactsCollection();
    check c->insertOne(f);
}

public function updateOrderFact(OrderFact f) returns error? {
    mongodb:Collection c = check orderFactsCollection();
    mongodb:UpdateResult _ = check c->updateOne({"orderId": f.orderId}, {set: f});
}

public function findOrderFact(string orderId) returns OrderFact?|error {
    mongodb:Collection c = check orderFactsCollection();
    stream<OrderFact, error?>|error resultOrError = c->find({"orderId": orderId}, targetType = OrderFact);
    stream<OrderFact, error?> result = check resultOrError;
    OrderFact? found = ();
    error? iterateError = from OrderFact f in result
        do {
            found = f;
        };
    if iterateError is error {
        return iterateError;
    }
    return found;
}

public function listOrderFacts() returns OrderFact[]|error {
    mongodb:Collection c = check orderFactsCollection();
    OrderFact[] facts = [];
    stream<OrderFact, error?>|error resultOrError = c->find({}, targetType = OrderFact);
    stream<OrderFact, error?> result = check resultOrError;
    error? iterateError = from OrderFact f in result
        do {
            facts.push(f);
        };
    if iterateError is error {
        return iterateError;
    }
    return facts;
}
