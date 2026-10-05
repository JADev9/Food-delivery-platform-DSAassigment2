// REST API and consumer startup for delivery-service.
//
// Drivers register, report location, and go on/off shift through HTTP.
// Driver assignment happens on the Kafka side, not through an endpoint:
// consumers.bal reacts to restaurant.order.ready and publishes the
// assignment. There is no POST /assign.

import ballerina/http;
import ballerina/log;
import ballerina/uuid;

service /api/v1 on new http:Listener(SERVICE_PORT) {

    resource function get health() returns json {
        return {status: "UP", 'service: SERVICE_NAME};
    }

    resource function post drivers(@http:Payload RegisterDriverRequest req) returns http:Created|http:InternalServerError {
        string driverId = uuid:createType4AsString();
        string now = nowUtc();
        Driver d = {
            driverId: driverId,
            name: req.name,
            phone: req.phone,
            vehicle: req.vehicle,
            location: req.location,
            locationUpdatedAt: now,
            status: DRIVER_AVAILABLE,
            currentDeliveryId: (),
            createdAt: now
        };
        error? saved = saveDriver(d);
        if saved is error {
            log:printError("failed to save driver", saved, driverId = driverId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not save driver"}
            };
        }
        return <http:Created>{body: d};
    }

    resource function get drivers(boolean availableOnly = false) returns DriverListResponse|http:InternalServerError {
        Driver[]|error found = listDrivers(availableOnly);
        if found is error {
            log:printError("failed to list drivers", found);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not list drivers"}
            };
        }
        return {items: found, total: found.length()};
    }

    resource function get drivers/[string driverId]() returns Driver|http:NotFound|http:InternalServerError {
        Driver?|error found = findDriver(driverId);
        if found is error {
            log:printError("failed to fetch driver", found, driverId = driverId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not fetch driver"}
            };
        }
        if found is () {
            return <http:NotFound>{
                body: {code: "NOT_FOUND", message: string `no driver ${driverId}`}
            };
        }
        return found;
    }

    resource function put drivers/[string driverId]/location(@http:Payload UpdateLocationRequest req) returns Driver|http:NotFound|http:InternalServerError {
        Driver?|error found = findDriver(driverId);
        if found is error {
            log:printError("failed to fetch driver", found, driverId = driverId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not fetch driver"}
            };
        }
        if found is () {
            return <http:NotFound>{
                body: {code: "NOT_FOUND", message: string `no driver ${driverId}`}
            };
        }
        Driver updated = found;
        updated.location = {latitude: req.latitude, longitude: req.longitude};
        updated.locationUpdatedAt = nowUtc();
        error? saved = updateDriver(updated);
        if saved is error {
            log:printError("failed to update location", saved, driverId = driverId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not update location"}
            };
        }
        return updated;
    }

    resource function put drivers/[string driverId]/status(@http:Payload UpdateStatusRequest req) returns Driver|http:NotFound|http:Conflict|http:InternalServerError {
        Driver?|error found = findDriver(driverId);
        if found is error {
            log:printError("failed to fetch driver", found, driverId = driverId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not fetch driver"}
            };
        }
        if found is () {
            return <http:NotFound>{
                body: {code: "NOT_FOUND", message: string `no driver ${driverId}`}
            };
        }
        Driver updated = found;
        // A driver on a delivery cannot simply be marked available or
        // off shift. The order must be completed or reassigned first.
        if updated.status == DRIVER_ON_DELIVERY && req.status != DRIVER_ON_DELIVERY {
            return <http:Conflict>{
                body: {code: "CONFLICT", message: "driver is on a delivery"}
            };
        }
        updated.status = req.status;
        error? saved = updateDriver(updated);
        if saved is error {
            log:printError("failed to update status", saved, driverId = driverId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not update status"}
            };
        }
        return updated;
    }

    resource function get deliveries(string? driverId, string? status) returns DeliveryListResponse|http:InternalServerError {
        Delivery[]|error found = listDeliveries(driverId, status);
        if found is error {
            log:printError("failed to list deliveries", found);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not list deliveries"}
            };
        }
        return {items: found, total: found.length()};
    }

    resource function get deliveries/'order/[string orderId]() returns Delivery|http:NotFound|http:InternalServerError {
        Delivery?|error found = findDeliveryByOrder(orderId);
        if found is error {
            log:printError("failed to find delivery", found, orderId = orderId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not find delivery"}
            };
        }
        if found is () {
            return <http:NotFound>{
                body: {code: "NOT_FOUND", message: string `no delivery for order ${orderId}`}
            };
        }
        return found;
    }

    resource function get deliveries/[string deliveryId]() returns Delivery|http:NotFound|http:InternalServerError {
        Delivery?|error found = findDelivery(deliveryId);
        if found is error {
            log:printError("failed to find delivery", found, deliveryId = deliveryId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not find delivery"}
            };
        }
        if found is () {
            return <http:NotFound>{
                body: {code: "NOT_FOUND", message: string `no delivery ${deliveryId}`}
            };
        }
        return found;
    }

    resource function post deliveries/[string deliveryId]/pickup() returns Delivery|http:NotFound|http:Conflict|http:InternalServerError {
        Delivery?|error found = findDelivery(deliveryId);
        if found is error {
            log:printError("failed to find delivery", found, deliveryId = deliveryId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not find delivery"}
            };
        }
        if found is () {
            return <http:NotFound>{
                body: {code: "NOT_FOUND", message: string `no delivery ${deliveryId}`}
            };
        }
        Delivery d = found;
        if d.status != DELIVERY_ASSIGNED {
            return <http:Conflict>{
                body: {code: "CONFLICT", message: string `delivery is ${d.status}, not ASSIGNED`}
            };
        }
        string now = nowUtc();
        d.status = DELIVERY_PICKED_UP;
        d.pickedUpAt = now;
        error? saved = updateDelivery(d);
        if saved is error {
            log:printError("failed to update delivery", saved, deliveryId = deliveryId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not update delivery"}
            };
        }
        DeliveryEvent outgoing = {
            eventId: newEventId(),
            eventType: TOPIC_DELIVERY_PICKED_UP,
            occurredAt: now,
            deliveryId: d.deliveryId,
            orderId: d.orderId,
            restaurantId: d.restaurantId,
            customerId: d.customerId,
            driverId: d.driverId,
            driverName: d.driverName,
            status: "PICKED_UP",
            reason: ()
        };
        error? published = publish(TOPIC_DELIVERY_PICKED_UP, outgoing, d.orderId);
        if published is error {
            log:printError("failed to publish delivery.picked-up", published, deliveryId = deliveryId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "delivery updated but event not published"}
            };
        }
        return d;
    }

    resource function post deliveries/[string deliveryId]/complete() returns Delivery|http:NotFound|http:Conflict|http:InternalServerError {
        Delivery?|error found = findDelivery(deliveryId);
        if found is error {
            log:printError("failed to find delivery", found, deliveryId = deliveryId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not find delivery"}
            };
        }
        if found is () {
            return <http:NotFound>{
                body: {code: "NOT_FOUND", message: string `no delivery ${deliveryId}`}
            };
        }
        Delivery d = found;
        if d.status != DELIVERY_PICKED_UP {
            return <http:Conflict>{
                body: {code: "CONFLICT", message: string `delivery is ${d.status}, not PICKED_UP`}
            };
        }
        string now = nowUtc();
        d.status = DELIVERY_COMPLETED;
        d.completedAt = now;
        error? saved = updateDelivery(d);
        if saved is error {
            log:printError("failed to update delivery", saved, deliveryId = deliveryId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not update delivery"}
            };
        }
        // Release the driver so the next ready order can pick them up.
        Driver?|error maybeDriver = findDriver(d.driverId);
        if maybeDriver is Driver {
            Driver drv = maybeDriver;
            drv.status = DRIVER_AVAILABLE;
            drv.currentDeliveryId = ();
            error? driverSaved = updateDriver(drv);
            if driverSaved is error {
                log:printError("failed to release driver", driverSaved, driverId = drv.driverId);
            }
        }
        DeliveryEvent outgoing = {
            eventId: newEventId(),
            eventType: TOPIC_DELIVERY_COMPLETED,
            occurredAt: now,
            deliveryId: d.deliveryId,
            orderId: d.orderId,
            restaurantId: d.restaurantId,
            customerId: d.customerId,
            driverId: d.driverId,
            driverName: d.driverName,
            status: "COMPLETED",
            reason: ()
        };
        error? published = publish(TOPIC_DELIVERY_COMPLETED, outgoing, d.orderId);
        if published is error {
            log:printError("failed to publish delivery.completed", published, deliveryId = deliveryId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "delivery updated but event not published"}
            };
        }
        return d;
    }
}

