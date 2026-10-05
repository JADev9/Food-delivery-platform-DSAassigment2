import ballerina/lang.runtime;
import ballerina/log;

public function main() returns error? {
    log:printInfo("restaurant-consumer starting");
    check startConsumers();
    while true {
        runtime:sleep(60);
    }
}
