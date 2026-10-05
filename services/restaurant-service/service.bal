// Restaurant service REST API.
//
// The kitchen queue is read through /tickets, and the ticket marked
// ready through /tickets/{orderId}/ready. That endpoint is the only one
// in this service that publishes an event.

import ballerina/http;
import ballerina/log;
import ballerina/uuid;

service /api/v1 on new http:Listener(SERVICE_PORT) {

    resource function get health() returns json {
        return {status: "UP", 'service: SERVICE_NAME};
    }

    resource function post restaurants(@http:Payload RegisterRestaurantRequest req) returns http:Created|http:InternalServerError {
        MenuItem[] menu = [];
        foreach NewMenuItem m in req.menu {
            menu.push({
                menuItemId: uuid:createType4AsString(),
                name: m.name,
                category: m.category,
                price: m.price,
                stock: m.stock
            });
        }

        string restaurantId = uuid:createType4AsString();
        Restaurant r = {
            restaurantId: restaurantId,
            name: req.name,
            address: req.address,
            phone: req.phone,
            cuisines: req.cuisines,
            location: req.location,
            hours: req.hours,
            accepting: true,
            menu: menu,
            createdAt: nowUtc()
        };
        error? saved = saveRestaurant(r);
        if saved is error {
            log:printError("failed to save restaurant", saved, restaurantId = restaurantId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not save restaurant"}
            };
        }
        return <http:Created>{body: r};
    }

    resource function get restaurants(boolean openOnly = false) returns RestaurantListResponse|http:InternalServerError {
        Restaurant[]|error found = listRestaurants(openOnly);
        if found is error {
            log:printError("failed to list restaurants", found);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not list restaurants"}
            };
        }
        return {items: found, total: found.length()};
    }

    resource function get restaurants/[string restaurantId]() returns Restaurant|http:NotFound|http:InternalServerError {
        Restaurant?|error found = findRestaurant(restaurantId);
        if found is error {
            log:printError("failed to find restaurant", found, restaurantId = restaurantId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not find restaurant"}
            };
        }
        if found is () {
            return <http:NotFound>{
                body: {code: "NOT_FOUND", message: string `no restaurant ${restaurantId}`}
            };
        }
        return found;
    }

    resource function get restaurants/[string restaurantId]/menu() returns MenuListResponse|http:NotFound|http:InternalServerError {
        Restaurant?|error found = findRestaurant(restaurantId);
        if found is error {
            log:printError("failed to find restaurant", found, restaurantId = restaurantId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not fetch menu"}
            };
        }
        if found is () {
            return <http:NotFound>{
                body: {code: "NOT_FOUND", message: string `no restaurant ${restaurantId}`}
            };
        }
        return {items: found.menu, total: found.menu.length()};
    }

    resource function post restaurants/[string restaurantId]/menu(@http:Payload AddMenuItemRequest req) returns http:Created|http:NotFound|http:InternalServerError {
        Restaurant?|error maybeRestaurant = findRestaurant(restaurantId);
        if maybeRestaurant is error {
            log:printError("failed to find restaurant", maybeRestaurant, restaurantId = restaurantId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not add dish"}
            };
        }
        if maybeRestaurant is () {
            return <http:NotFound>{
                body: {code: "NOT_FOUND", message: string `no restaurant ${restaurantId}`}
            };
        }

        Restaurant r = maybeRestaurant;
        MenuItem item = {
            menuItemId: uuid:createType4AsString(),
            name: req.name,
            category: req.category,
            price: req.price,
            stock: req.stock
        };
        r.menu.push(item);
        error? saved = updateRestaurant(r);
        if saved is error {
            log:printError("failed to update restaurant", saved, restaurantId = restaurantId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not add dish"}
            };
        }
        return <http:Created>{body: item};
    }

    resource function put restaurants/[string restaurantId]/menu/[string menuItemId]/stock(@http:Payload SetStockRequest req) returns MenuItem|http:NotFound|http:InternalServerError {
        Restaurant?|error maybeRestaurant = findRestaurant(restaurantId);
        if maybeRestaurant is error {
            log:printError("failed to find restaurant", maybeRestaurant, restaurantId = restaurantId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not set stock"}
            };
        }
        if maybeRestaurant is () {
            return <http:NotFound>{
                body: {code: "NOT_FOUND", message: string `no restaurant ${restaurantId}`}
            };
        }

        Restaurant r = maybeRestaurant;
        boolean found = false;
        MenuItem updated = {menuItemId: "", name: "", category: "", price: 0d, stock: 0};
        foreach int i in 0 ..< r.menu.length() {
            if r.menu[i].menuItemId == menuItemId {
                r.menu[i].stock = req.stock;
                updated = r.menu[i];
                found = true;
                break;
            }
        }
        if !found {
            return <http:NotFound>{
                body: {code: "NOT_FOUND", message: string `no menu item ${menuItemId}`}
            };
        }

        error? saved = updateRestaurant(r);
        if saved is error {
            log:printError("failed to update restaurant", saved, restaurantId = restaurantId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not set stock"}
            };
        }
        return updated;
    }

    resource function put restaurants/[string restaurantId]/availability(boolean accepting) returns Restaurant|http:NotFound|http:InternalServerError {
        Restaurant?|error maybeRestaurant = findRestaurant(restaurantId);
        if maybeRestaurant is error {
            log:printError("failed to find restaurant", maybeRestaurant, restaurantId = restaurantId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not set availability"}
            };
        }
        if maybeRestaurant is () {
            return <http:NotFound>{
                body: {code: "NOT_FOUND", message: string `no restaurant ${restaurantId}`}
            };
        }

        Restaurant r = maybeRestaurant;
        r.accepting = accepting;
        error? saved = updateRestaurant(r);
        if saved is error {
            log:printError("failed to update restaurant", saved, restaurantId = restaurantId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not set availability"}
            };
        }
        return r;
    }

    resource function get restaurants/[string restaurantId]/tickets(string? status) returns TicketListResponse|http:InternalServerError {
        Ticket[]|error found = listTickets(restaurantId, status);
        if found is error {
            log:printError("failed to list tickets", found, restaurantId = restaurantId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not list tickets"}
            };
        }
        return {items: found, total: found.length()};
    }

    resource function post tickets/[string orderId]/ready() returns Ticket|http:NotFound|http:Conflict|http:InternalServerError {
        Ticket?|error maybeTicket = findTicket(orderId);
        if maybeTicket is error {
            log:printError("failed to find ticket", maybeTicket, orderId = orderId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not mark ready"}
            };
        }
        if maybeTicket is () {
            return <http:NotFound>{
                body: {code: "NOT_FOUND", message: string `no ticket for order ${orderId}`}
            };
        }

        Ticket t = maybeTicket;
        if t.status == TICKET_READY {
            // Idempotent: already marked ready.
            return t;
        }
        if t.status != TICKET_ACCEPTED {
            return <http:Conflict>{
                body: {code: "CONFLICT", message: string `ticket is ${t.status}, not ACCEPTED`}
            };
        }

        string now = nowUtc();
        t.status = TICKET_READY;
        t.readyAt = now;
        error? saved = updateTicket(t);
        if saved is error {
            log:printError("failed to update ticket", saved, orderId = orderId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not mark ready"}
            };
        }

        KitchenEvent outgoing = {
            eventId: newEventId(),
            eventType: TOPIC_KITCHEN_READY,
            occurredAt: now,
            orderId: t.orderId,
            restaurantId: t.restaurantId,
            status: TICKET_READY,
            prepTimeMinutes: t.prepTimeMinutes,
            reason: ()
        };
        error? published = publish(TOPIC_KITCHEN_READY, outgoing, t.orderId);
        if published is error {
            log:printError("failed to publish kitchen ready", published, orderId = orderId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "ticket updated but event not published"}
            };
        }
        log:printInfo("food ready", orderId = orderId);
        return t;
    }
}
