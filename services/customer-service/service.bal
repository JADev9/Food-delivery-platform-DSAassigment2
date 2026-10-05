// Customer service REST API.
//
// The order-history endpoint proxies live to order-service. The
// customer database holds only a cached lifetime-spend number, not the
// orders themselves, so there is nothing to keep in sync.

import ballerina/http;
import ballerina/log;
import ballerina/uuid;

final http:Client orderClient = check new (ORDER_SERVICE_URL, {timeout: 5});

service /api/v1 on new http:Listener(SERVICE_PORT) {

    resource function get health() returns json {
        return {status: "UP", 'service: SERVICE_NAME};
    }

    resource function post customers(@http:Payload RegisterCustomerRequest req) returns http:Created|http:Conflict|http:InternalServerError {
        Customer?|error existing = findCustomerByEmail(req.email);
        if existing is error {
            log:printError("failed to check email", existing);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not register customer"}
            };
        }
        if existing is Customer {
            return <http:Conflict>{
                body: {code: "CONFLICT", message: "email already registered"}
            };
        }

        string customerId = uuid:createType4AsString();
        string now = nowUtc();
        Customer c = {
            customerId: customerId,
            name: req.name,
            email: req.email,
            phone: req.phone,
            lifetimeOrders: 0,
            lifetimeSpend: 0d,
            createdAt: now,
            updatedAt: now
        };
        error? saved = saveCustomer(c);
        if saved is error {
            log:printError("failed to save customer", saved, customerId = customerId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not save customer"}
            };
        }
        return <http:Created>{body: c};
    }

    resource function get customers() returns CustomerListResponse|http:InternalServerError {
        Customer[]|error found = listCustomers();
        if found is error {
            log:printError("failed to list customers", found);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not list customers"}
            };
        }
        return {items: found, total: found.length()};
    }

    resource function get customers/[string customerId]() returns Customer|http:NotFound|http:InternalServerError {
        Customer?|error found = findCustomer(customerId);
        if found is error {
            log:printError("failed to find customer", found, customerId = customerId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not find customer"}
            };
        }
        if found is () {
            return <http:NotFound>{
                body: {code: "NOT_FOUND", message: string `no customer ${customerId}`}
            };
        }
        return found;
    }

    resource function post customers/[string customerId]/addresses(@http:Payload SaveAddressRequest req) returns http:Created|http:NotFound|http:InternalServerError {
        Customer?|error customer = findCustomer(customerId);
        if customer is error {
            log:printError("failed to check customer", customer);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not save address"}
            };
        }
        if customer is () {
            return <http:NotFound>{
                body: {code: "NOT_FOUND", message: string `no customer ${customerId}`}
            };
        }

        if req.isDefault {
            error? cleared = clearDefaultAddress(customerId);
            if cleared is error {
                log:printError("failed to clear previous default address", cleared, customerId = customerId);
                return <http:InternalServerError>{
                    body: {code: "INTERNAL", message: "could not save address"}
                };
            }
        }

        string addressId = uuid:createType4AsString();
        Address a = {
            addressId: addressId,
            customerId: customerId,
            label: req.label,
            line1: req.line1,
            suburb: req.suburb,
            city: req.city,
            instructions: req.instructions,
            isDefault: req.isDefault,
            createdAt: nowUtc()
        };
        error? saved = saveAddress(a);
        if saved is error {
            log:printError("failed to save address", saved, addressId = addressId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not save address"}
            };
        }
        return <http:Created>{body: a};
    }

    resource function get customers/[string customerId]/addresses() returns AddressListResponse|http:InternalServerError {
        Address[]|error found = listAddresses(customerId);
        if found is error {
            log:printError("failed to list addresses", found, customerId = customerId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not list addresses"}
            };
        }
        return {items: found, total: found.length()};
    }

    resource function get customers/[string customerId]/orders() returns json|http:NotFound|http:BadGateway|http:InternalServerError {
        Customer?|error found = findCustomer(customerId);
        if found is error {
            log:printError("failed to check customer", found, customerId = customerId);
            return <http:InternalServerError>{
                body: {code: "INTERNAL", message: "could not fetch order history"}
            };
        }
        if found is () {
            return <http:NotFound>{
                body: {code: "NOT_FOUND", message: string `no customer ${customerId}`}
            };
        }

        string path = string `/api/v1/orders?customerId=${customerId}`;
        json|error response = orderClient->get(path);
        if response is error {
            log:printError("order-service lookup failed", response, customerId = customerId);
            return <http:BadGateway>{
                body: {code: "UPSTREAM_FAILED", message: "order-service unreachable"}
            };
        }
        return response;
    }
}
