import ballerina/lang.runtime;
import ballerina/log;

public function main() returns error? {
    log:printInfo("admin-consumer (delivery) starting");
    check startConsumers();
    while true {
        runtime:sleep(60);
    }
}
