// Nearest-driver dispatch.
//
// When the kitchen marks an order ready, this file picks the driver
// closest to the restaurant. It is the only place in the platform where
// one service calls another over HTTP synchronously: a driver needs the
// restaurant's current coordinates, and a cached event would go stale.

import ballerina/http;
import ballerina/lang.'float;
import ballerina/log;

final http:Client restaurantClient = check new (RESTAURANT_SERVICE_URL, {timeout: 3});


isolated function restaurantLocation(string restaurantId) returns GeoPoint {
    string path = string `/api/v1/restaurants/${restaurantId}`;
    json|error response = restaurantClient->get(path);
    if response is error {
        log:printWarn("restaurant lookup failed, using fallback",
                      restaurantId = restaurantId, detail = response.message());
        return fallbackPickup();
    }

    json|error latOrErr = response.latitude;
    if latOrErr is error {
        return fallbackPickup();
    }
    json|error lonOrErr = response.longitude;
    if lonOrErr is error {
        return fallbackPickup();
    }

    json lat = latOrErr;
    json lon = lonOrErr;
    if lat !is string && lat !is decimal && lat !is float && lat !is int {
        return fallbackPickup();
    }
    if lon !is string && lon !is decimal && lon !is float && lon !is int {
        return fallbackPickup();
    }
    decimal latVal = <decimal>lat;
    decimal lonVal = <decimal>lon;
    return {latitude: latVal, longitude: lonVal};
}

isolated function distanceKm(GeoPoint a, GeoPoint b) returns decimal {
    float lat1 = <float>a.latitude * 0.017453292519943295;
    float lat2 = <float>b.latitude * 0.017453292519943295;
    float dLat = (<float>b.latitude - <float>a.latitude) * 0.017453292519943295;
    float dLon = (<float>b.longitude - <float>a.longitude) * 0.017453292519943295;

    // Haversine. decimal has no sin/cos, so the trig runs in float and
    // the result converts back.
    float sinHalfLat = 'float:sin(dLat / 2.0);
    float sinHalfLon = 'float:sin(dLon / 2.0);
    float a2 = sinHalfLat * sinHalfLat +
               'float:cos(lat1) * 'float:cos(lat2) * sinHalfLon * sinHalfLon;
    float c = 2.0 * 'float:atan2('float:sqrt(a2), 'float:sqrt(1.0 - a2));
    float km = 6371.0 * c;
    return <decimal>km;
}

isolated function findNearestDriver(GeoPoint pickup) returns Driver|error {
    Driver[] available = check listDrivers(true);
    if available.length() == 0 {
        return error("no available drivers");
    }

    Driver best = available[0];
    decimal bestDistance = distanceKm(best.location, pickup);

    foreach Driver d in available {
        decimal dist = distanceKm(d.location, pickup);
        if dist < bestDistance {
            best = d;
            bestDistance = dist;
        }
    }
    return best;
}

isolated function fallbackPickup() returns GeoPoint {
    return {latitude: -22.5735, longitude: 17.0857};
}

isolated function toDecimal(json v) returns decimal|error {
    if v is string {
        return decimal:fromString(v);
    }
    if v is int|float|decimal {
        return <decimal>v;
    }
    return error("not a number");
}
