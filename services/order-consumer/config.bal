// Runtime configuration. Every value is read from an environment variable
// with a default, so the same image runs unchanged under Docker Compose,
// Kubernetes, or `bal run`.

import ballerina/os;

// Reads an environment variable, falling back when it is unset or blank.
//
// + name - environment variable name
// + fallback - value to use when the variable is absent
// + return - the resolved value
isolated function env(string name, string fallback) returns string {
    string value = os:getEnv(name);
    return value.trim() == "" ? fallback : value;
}

// Reads an integer environment variable, falling back when unset or unparseable.
//
// + name - environment variable name
// + fallback - value to use when the variable is absent or invalid
// + return - the resolved value
isolated function envInt(string name, int fallback) returns int {
    string value = os:getEnv(name).trim();
    if value == "" {
        return fallback;
    }
    int|error parsed = int:fromString(value);
    return parsed is int ? parsed : fallback;
}

final string SERVICE_NAME = "order-service";
final string KAFKA_BOOTSTRAP = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
final string MONGO_HOST = env("MONGO_HOST", "mongodb");
final int MONGO_PORT = envInt("MONGO_PORT", 27017);
final string MONGO_USER = env("MONGO_USER", "root");
final string MONGO_PASSWORD = env("MONGO_PASSWORD", "dsa612s");
final string MONGO_DB = env("MONGO_DB", "orders");
final int SERVICE_PORT = envInt("SERVICE_PORT", 8083);
