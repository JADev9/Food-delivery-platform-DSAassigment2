import ballerina/log;

public function main() returns error? {
    log:printInfo("delivery-consumer starting");
    return startConsumers();
}
