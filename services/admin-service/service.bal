import ballerina/http;
import ballerina/log;
import ballerina/time;

type DriverAccumulator record {|
    string driverId;
    string driverName;
    int totalDeliveries;
    decimal totalMinutes;
|};

service /api/v1 on new http:Listener(SERVICE_PORT) {

    resource function get health() returns json {
        return {status: "UP", 'service: SERVICE_NAME};
    }

    resource function get reports/summary() returns SummaryReport|http:InternalServerError {
        OrderFact[]|error facts = listOrderFacts();
        if facts is error {
            log:printError("failed to load order facts", facts);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not load reports"}
            };
        }

        int delivered = 0;
        int cancelled = 0;
        int inProgress = 0;
        decimal revenue = 0d;

        foreach OrderFact f in facts {
            if f.status == "DELIVERED" {
                delivered += 1;
                revenue += f.totalAmount;
            } else if f.status == "CANCELLED" {
                cancelled += 1;
            } else {
                inProgress += 1;
            }
        }

        int total = facts.length();
        decimal average = total > 0 ? revenue / <decimal>delivered : 0d;

        return {
            totalOrders: total,
            delivered: delivered,
            cancelled: cancelled,
            inProgress: inProgress,
            grossRevenue: revenue,
            averageOrderValue: average,
            generatedAt: time:utcToString(time:utcNow())
        };
    }

    resource function get reports/restaurants() returns RestaurantReport[]|http:InternalServerError {
        OrderFact[]|error facts = listOrderFacts();
        if facts is error {
            log:printError("failed to load order facts", facts);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not load reports"}
            };
        }

        map<RestaurantReport> byRestaurant = {};

        foreach OrderFact f in facts {
            RestaurantReport? existing = byRestaurant[f.restaurantId];
            RestaurantReport r = existing is () ? {
                restaurantId: f.restaurantId,
                restaurantName: f.restaurantName == "" ? f.restaurantId : f.restaurantName,
                totalOrders: 0,
                delivered: 0,
                cancelled: 0,
                revenue: 0d
            } : existing;

            r.totalOrders += 1;
            if f.status == "DELIVERED" {
                r.delivered += 1;
                r.revenue += f.totalAmount;
            } else if f.status == "CANCELLED" {
                r.cancelled += 1;
            }
            byRestaurant[f.restaurantId] = r;
        }

        RestaurantReport[] reports = [];
        foreach var entry in byRestaurant.entries() {
            RestaurantReport r = entry[1];
            reports.push(r);
        }
        return reports;
    }

    resource function get reports/drivers() returns DriverReport[]|http:InternalServerError {
        OrderFact[]|error facts = listOrderFacts();
        if facts is error {
            log:printError("failed to load order facts", facts);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not load reports"}
            };
        }

        map<DriverAccumulator> byDriver = {};

        foreach OrderFact f in facts {
            string? maybeDriverId = f.driverId;
            if maybeDriverId is () || maybeDriverId == "" {
                continue;
            }
            if f.status != "DELIVERED" {
                continue;
            }
            string driverId = maybeDriverId;
            string driverName = f.driverName ?: driverId;

            DriverAccumulator? existing = byDriver[driverId];
            DriverAccumulator acc = existing is () ? {
                driverId: driverId,
                driverName: driverName,
                totalDeliveries: 0,
                totalMinutes: 0d
            } : existing;
            acc.totalDeliveries += 1;

            string? assignedAt = f.assignedAt;
            string? completedAt = f.completedAt;
            if assignedAt is string && completedAt is string {
                acc.totalMinutes += minutesBetween(assignedAt, completedAt);
            }
            byDriver[driverId] = acc;
        }

        DriverReport[] reports = [];
        foreach var entry in byDriver.entries() {
            DriverAccumulator acc = entry[1];
            decimal avg = acc.totalDeliveries > 0
                ? acc.totalMinutes / <decimal>acc.totalDeliveries
                : 0d;
            reports.push({
                driverId: acc.driverId,
                driverName: acc.driverName,
                totalDeliveries: acc.totalDeliveries,
                averageDeliveryMinutes: avg
            });
        }
        return reports;
    }

    resource function get reports/delivery/performance() returns DeliveryPerformanceReport|http:InternalServerError {
        OrderFact[]|error facts = listOrderFacts();
        if facts is error {
            log:printError("failed to load order facts", facts);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not load reports"}
            };
        }

        decimal totalOrderToDelivery = 0d;
        decimal totalReadyToDelivery = 0d;
        int orderSample = 0;
        int readySample = 0;

        foreach OrderFact f in facts {
            if f.status != "DELIVERED" {
                continue;
            }
            string? maybeCompleted = f.completedAt;
            if maybeCompleted is () {
                continue;
            }
            string completedAt = maybeCompleted;

            decimal orderMinutes = minutesBetween(f.createdAt, completedAt);
            totalOrderToDelivery += orderMinutes;
            orderSample += 1;

            string? maybePickedUp = f.pickedUpAt;
            if maybePickedUp is string {
                decimal readyMinutes = minutesBetween(maybePickedUp, completedAt);
                totalReadyToDelivery += readyMinutes;
                readySample += 1;
            }
        }

        decimal avgOrderToDelivery = orderSample > 0
            ? totalOrderToDelivery / <decimal>orderSample
            : 0d;
        decimal avgReadyToDelivery = readySample > 0
            ? totalReadyToDelivery / <decimal>readySample
            : 0d;

        return {
            averageOrderToDeliveryMinutes: avgOrderToDelivery,
            averageReadyToDeliveryMinutes: avgReadyToDelivery,
            sampleSize: orderSample,
            generatedAt: time:utcToString(time:utcNow())
        };
    }

    resource function get reports/orders() returns OrderListResponse|http:InternalServerError {
        OrderFact[]|error facts = listOrderFacts();
        if facts is error {
            log:printError("failed to load order facts", facts);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not load reports"}
            };
        }
        return {items: facts, total: facts.length()};
    }
}

isolated function minutesBetween(string fromIso, string toIso) returns decimal {
    time:Utc|error startUtc = time:utcFromString(fromIso);
    if startUtc is error {
        return 0d;
    }
    time:Utc|error endUtc = time:utcFromString(toIso);
    if endUtc is error {
        return 0d;
    }
    decimal seconds = <decimal>endUtc[0] - <decimal>startUtc[0];
    return seconds / 60d;
}
