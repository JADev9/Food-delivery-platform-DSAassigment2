// Standalone consumer process for order-service.
//
// The REST API and the Kafka consumers run as two Ballerina processes
// because Ballerina's scheduler cannot serve HTTP requests while three
// blocking consumer loops are holding its strands. Splitting them is
// the standard pattern for this shape of workload.

import ballerina/log;

public function main() returns error? {
    log:printInfo("order-consumer starting");
    return startConsumers();
}
