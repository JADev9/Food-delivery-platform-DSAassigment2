// MongoDB persistence for delivery-service.
//
// Two collections:
//   - drivers     the driver pool, with live position and availability
//   - deliveries  one record per order, from assignment to handover
//
// Only this file talks to MongoDB.

import ballerina/log;
import ballerinax/mongodb;

final mongodb:Client mongoClient = check new ({
    connection: string `mongodb://${MONGO_USER}:${MONGO_PASSWORD}@${MONGO_HOST}:${MONGO_PORT}/?authSource=admin`
});

isolated function driversCollection() returns mongodb:Collection|error {
    mongodb:Database db = check mongoClient->getDatabase(MONGO_DB);
    return check db->getCollection("drivers");
}

isolated function deliveriesCollection() returns mongodb:Collection|error {
    mongodb:Database db = check mongoClient->getDatabase(MONGO_DB);
    return check db->getCollection("deliveries");
}

public function saveDriver(Driver d) returns error? {
    mongodb:Collection c = check driversCollection();
    check c->insertOne(d);
    log:printDebug("driver saved", driverId = d.driverId);
}

public function updateDriver(Driver d) returns error? {
    mongodb:Collection c = check driversCollection();
    mongodb:UpdateResult _ = check c->updateOne({"driverId": d.driverId}, {set: d});
    log:printDebug("driver updated", driverId = d.driverId, status = d.status);
}

public function findDriver(string driverId) returns Driver?|error {
    mongodb:Collection c = check driversCollection();
    stream<Driver, error?>|error resultOrError = c->find({"driverId": driverId}, targetType = Driver);
    stream<Driver, error?> result = check resultOrError;
    Driver? found = ();
    error? iterateError = from Driver d in result
        do {
            found = d;
        };
    if iterateError is error {
        return iterateError;
    }
    return found;
}

public isolated function listDrivers(boolean availableOnly) returns Driver[]|error {
    mongodb:Collection c = check driversCollection();
    map<json> filter = {};
    if availableOnly {
        filter["status"] = DRIVER_AVAILABLE;
    }
    Driver[] drivers = [];
    stream<Driver, error?>|error resultOrError = c->find(filter, targetType = Driver);
    stream<Driver, error?> result = check resultOrError;
    error? iterateError = from Driver d in result
        do {
            drivers.push(d);
        };
    if iterateError is error {
        return iterateError;
    }
    return drivers;
}

public function saveDelivery(Delivery d) returns error? {
    mongodb:Collection c = check deliveriesCollection();
    check c->insertOne(d);
    log:printDebug("delivery saved", deliveryId = d.deliveryId, orderId = d.orderId);
}

public function updateDelivery(Delivery d) returns error? {
    mongodb:Collection c = check deliveriesCollection();
    mongodb:UpdateResult _ = check c->updateOne({"deliveryId": d.deliveryId}, {set: d});
    log:printDebug("delivery updated", deliveryId = d.deliveryId, status = d.status);
}

public function findDelivery(string deliveryId) returns Delivery?|error {
    mongodb:Collection c = check deliveriesCollection();
    stream<Delivery, error?>|error resultOrError = c->find({"deliveryId": deliveryId}, targetType = Delivery);
    stream<Delivery, error?> result = check resultOrError;
    Delivery? found = ();
    error? iterateError = from Delivery d in result
        do {
            found = d;
        };
    if iterateError is error {
        return iterateError;
    }
    return found;
}

public function findDeliveryByOrder(string orderId) returns Delivery?|error {
    mongodb:Collection c = check deliveriesCollection();
    stream<Delivery, error?>|error resultOrError = c->find({"orderId": orderId}, targetType = Delivery);
    stream<Delivery, error?> result = check resultOrError;
    Delivery? found = ();
    error? iterateError = from Delivery d in result
        do {
            found = d;
        };
    if iterateError is error {
        return iterateError;
    }
    return found;
}

public function listDeliveries(string? driverId, string? status) returns Delivery[]|error {
    mongodb:Collection c = check deliveriesCollection();
    map<json> filter = {};
    if driverId is string {
        filter["driverId"] = driverId;
    }
    if status is string {
        filter["status"] = status;
    }
    Delivery[] deliveries = [];
    stream<Delivery, error?>|error resultOrError = c->find(filter, targetType = Delivery);
    stream<Delivery, error?> result = check resultOrError;
    error? iterateError = from Delivery d in result
        do {
            deliveries.push(d);
        };
    if iterateError is error {
        return iterateError;
    }
    return deliveries;
}
