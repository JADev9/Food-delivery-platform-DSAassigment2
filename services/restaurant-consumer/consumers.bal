// Stock reservation and ticket queue.
//
// reserveStock checks every line before committing any, so an order
// cannot half-reserve. restoreStock puts everything back on cancellation.

import ballerina/lang.value;
import ballerina/log;
import ballerinax/kafka;

public function startConsumers() returns error? {
    check ordersConsumer->subscribe([TOPIC_ORDERS_CONFIRMED, TOPIC_ORDERS_CANCELLED]);
    log:printInfo("consumers started");
    return ordersLoop();
}

function ordersLoop() returns error? {
    while true {
        kafka:AnydataConsumerRecord[] records = check ordersConsumer->poll(1.0);
        foreach kafka:AnydataConsumerRecord rec in records {
            do {
                check handleOrderRecord(rec);
            } on fail error e {
                log:printError("restaurant handler failed", orderId = extractOrderId(rec));
            }
        }
        error? commitError = ordersConsumer->'commit();
        if commitError is error {
            log:printError("commit failed", commitError);
        }
    }
}

function handleOrderRecord(kafka:AnydataConsumerRecord rec) returns error? {
    json payload = check decodeRecord(rec);
    OrderEvent event = check payload.cloneWithType(OrderEvent);

    if event.eventType == TOPIC_ORDERS_CONFIRMED {
        return acceptOrder(event);
    }
    if event.eventType == TOPIC_ORDERS_CANCELLED {
        return cancelOrder(event);
    }
}

function acceptOrder(OrderEvent event) returns error? {
    // Idempotency: if a reservation already exists, we have seen this.
    Reservation? existing = check findReservation(event.orderId);
    if existing is Reservation {
        log:printInfo("reservation already exists", orderId = event.orderId);
        return;
    }

    Restaurant? maybeRestaurant = check findRestaurant(event.restaurantId);
    if maybeRestaurant is () {
        return rejectOrder(event, "restaurant not found");
    }
    Restaurant restaurant = maybeRestaurant;

    if !restaurant.accepting {
        return rejectOrder(event, "kitchen is closed");
    }

    // Check every item has enough stock before reserving any. This is
    // what keeps the reservation atomic.
    foreach OrderItem item in event.items {
        int available = stockOf(restaurant, item.menuItemId);
        if available < item.quantity {
            string reason = string `insufficient stock for ${item.name}: ${available} available, ${item.quantity} needed`;
            return rejectOrder(event, reason);
        }
    }

    // Reserve. Update the restaurant's menu in one write.
    Restaurant updated = restaurant;
    TicketItem[] reserved = [];
    foreach OrderItem item in event.items {
        decreaseStock(updated, item.menuItemId, item.quantity);
        reserved.push({
            menuItemId: item.menuItemId,
            name: item.name,
            quantity: item.quantity
        });
    }
    check updateRestaurant(updated);

    string now = nowUtc();
    Reservation reservation = {
        orderId: event.orderId,
        itemsReserved: reserved,
        reservedAt: now,
        restored: false
    };
    check saveReservation(reservation);

    Ticket ticket = {
        orderId: event.orderId,
        restaurantId: event.restaurantId,
        items: reserved,
        status: TICKET_ACCEPTED,
        prepTimeMinutes: DEFAULT_PREP_MINUTES,
        receivedAt: now,
        readyAt: ()
    };
    check saveTicket(ticket);

    KitchenEvent outgoing = {
        eventId: newEventId(),
        eventType: TOPIC_KITCHEN_ACCEPTED,
        occurredAt: now,
        orderId: event.orderId,
        restaurantId: event.restaurantId,
        status: TICKET_ACCEPTED,
        prepTimeMinutes: DEFAULT_PREP_MINUTES,
        reason: ()
    };
    check publish(TOPIC_KITCHEN_ACCEPTED, outgoing, event.orderId);
    log:printInfo("order accepted", orderId = event.orderId);
}

function rejectOrder(OrderEvent event, string reason) returns error? {
    string now = nowUtc();
    KitchenEvent outgoing = {
        eventId: newEventId(),
        eventType: TOPIC_KITCHEN_ACCEPTED,
        occurredAt: now,
        orderId: event.orderId,
        restaurantId: event.restaurantId,
        status: TICKET_REJECTED,
        prepTimeMinutes: 0,
        reason: reason
    };
    check publish(TOPIC_KITCHEN_ACCEPTED, outgoing, event.orderId);
    log:printWarn("order rejected", orderId = event.orderId, reason = reason);
}

function cancelOrder(OrderEvent event) returns error? {
    Reservation? maybeReservation = check findReservation(event.orderId);
    if maybeReservation is () {
        // Nothing was reserved for this order. Nothing to restore.
        return;
    }

    Reservation reservation = maybeReservation;
    if reservation.restored {
        return;
    }

    Restaurant? maybeRestaurant = check findRestaurant(event.restaurantId);
    if maybeRestaurant is () {
        return;
    }

    Restaurant updated = maybeRestaurant;
    foreach TicketItem item in reservation.itemsReserved {
        increaseStock(updated, item.menuItemId, item.quantity);
    }
    check updateRestaurant(updated);

    reservation.restored = true;
    check updateReservation(reservation);

    log:printInfo("stock restored", orderId = event.orderId);
}

isolated function stockOf(Restaurant r, string menuItemId) returns int {
    foreach MenuItem item in r.menu {
        if item.menuItemId == menuItemId {
            return item.stock;
        }
    }
    return 0;
}

isolated function decreaseStock(Restaurant r, string menuItemId, int quantity) {
    foreach int i in 0 ..< r.menu.length() {
        if r.menu[i].menuItemId == menuItemId {
            r.menu[i].stock = r.menu[i].stock - quantity;
            return;
        }
    }
}

isolated function increaseStock(Restaurant r, string menuItemId, int quantity) {
    foreach int i in 0 ..< r.menu.length() {
        if r.menu[i].menuItemId == menuItemId {
            r.menu[i].stock = r.menu[i].stock + quantity;
            return;
        }
    }
}

isolated function decodeRecord(kafka:AnydataConsumerRecord rec) returns json|error {
    byte[] raw = check rec.value.ensureType();
    string text = check string:fromBytes(raw);
    return value:fromJsonString(text);
}

isolated function extractOrderId(kafka:AnydataConsumerRecord rec) returns string {
    byte[]|error raw = rec.value.ensureType();
    if raw is error {
        return "unknown";
    }
    string|error text = string:fromBytes(raw);
    if text is error {
        return "unknown";
    }
    json|error parsed = value:fromJsonString(text);
    if parsed is error {
        return "unknown";
    }
    if parsed is map<json> {
        json v = parsed["orderId"] ?: "unknown";
        return v is string ? v : "unknown";
    }
    return "unknown";
}
