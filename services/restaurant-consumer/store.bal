import ballerinax/mongodb;

final mongodb:Client mongoClient = check new ({
    connection: string `mongodb://${MONGO_USER}:${MONGO_PASSWORD}@${MONGO_HOST}:${MONGO_PORT}/?authSource=admin`
});

isolated function restaurantsCollection() returns mongodb:Collection|error {
    mongodb:Database db = check mongoClient->getDatabase(MONGO_DB);
    return check db->getCollection("restaurants");
}

isolated function ticketsCollection() returns mongodb:Collection|error {
    mongodb:Database db = check mongoClient->getDatabase(MONGO_DB);
    return check db->getCollection("tickets");
}

isolated function reservationsCollection() returns mongodb:Collection|error {
    mongodb:Database db = check mongoClient->getDatabase(MONGO_DB);
    return check db->getCollection("reservations");
}

public function saveRestaurant(Restaurant r) returns error? {
    mongodb:Collection c = check restaurantsCollection();
    check c->insertOne(r);
}

public function updateRestaurant(Restaurant r) returns error? {
    mongodb:Collection c = check restaurantsCollection();
    mongodb:UpdateResult _ = check c->updateOne({"restaurantId": r.restaurantId}, {set: r});
}

public function findRestaurant(string restaurantId) returns Restaurant?|error {
    mongodb:Collection c = check restaurantsCollection();
    stream<Restaurant, error?>|error resultOrError = c->find({"restaurantId": restaurantId}, targetType = Restaurant);
    stream<Restaurant, error?> result = check resultOrError;
    Restaurant? found = ();
    error? iterateError = from Restaurant r in result
        do {
            found = r;
        };
    if iterateError is error {
        return iterateError;
    }
    return found;
}

public function listRestaurants(boolean openOnly) returns Restaurant[]|error {
    mongodb:Collection c = check restaurantsCollection();
    map<json> filter = {};
    if openOnly {
        filter["accepting"] = true;
    }
    Restaurant[] items = [];
    stream<Restaurant, error?>|error resultOrError = c->find(filter, targetType = Restaurant);
    stream<Restaurant, error?> result = check resultOrError;
    error? iterateError = from Restaurant r in result
        do {
            items.push(r);
        };
    if iterateError is error {
        return iterateError;
    }
    return items;
}

public function saveTicket(Ticket t) returns error? {
    mongodb:Collection c = check ticketsCollection();
    check c->insertOne(t);
}

public function updateTicket(Ticket t) returns error? {
    mongodb:Collection c = check ticketsCollection();
    mongodb:UpdateResult _ = check c->updateOne({"orderId": t.orderId}, {set: t});
}

public function findTicket(string orderId) returns Ticket?|error {
    mongodb:Collection c = check ticketsCollection();
    stream<Ticket, error?>|error resultOrError = c->find({"orderId": orderId}, targetType = Ticket);
    stream<Ticket, error?> result = check resultOrError;
    Ticket? found = ();
    error? iterateError = from Ticket t in result
        do {
            found = t;
        };
    if iterateError is error {
        return iterateError;
    }
    return found;
}

public function listTickets(string restaurantId, string? status) returns Ticket[]|error {
    mongodb:Collection c = check ticketsCollection();
    map<json> filter = {"restaurantId": restaurantId};
    if status is string {
        filter["status"] = status;
    }
    Ticket[] items = [];
    stream<Ticket, error?>|error resultOrError = c->find(filter, targetType = Ticket);
    stream<Ticket, error?> result = check resultOrError;
    error? iterateError = from Ticket t in result
        do {
            items.push(t);
        };
    if iterateError is error {
        return iterateError;
    }
    return items;
}

public function saveReservation(Reservation r) returns error? {
    mongodb:Collection c = check reservationsCollection();
    check c->insertOne(r);
}

public function updateReservation(Reservation r) returns error? {
    mongodb:Collection c = check reservationsCollection();
    mongodb:UpdateResult _ = check c->updateOne({"orderId": r.orderId}, {set: r});
}

public function findReservation(string orderId) returns Reservation?|error {
    mongodb:Collection c = check reservationsCollection();
    stream<Reservation, error?>|error resultOrError = c->find({"orderId": orderId}, targetType = Reservation);
    stream<Reservation, error?> result = check resultOrError;
    Reservation? found = ();
    error? iterateError = from Reservation r in result
        do {
            found = r;
        };
    if iterateError is error {
        return iterateError;
    }
    return found;
}
