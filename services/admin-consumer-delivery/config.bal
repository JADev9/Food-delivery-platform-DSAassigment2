import ballerina/os;

isolated function env(string name, string fallback) returns string {
    string value = os:getEnv(name);
    return value.trim() == "" ? fallback : value;
}

isolated function envInt(string name, int fallback) returns int {
    string value = os:getEnv(name).trim();
    if value == "" {
        return fallback;
    }
    int|error parsed = int:fromString(value);
    return parsed is int ? parsed : fallback;
}

final string SERVICE_NAME = "admin-service";
final string KAFKA_BOOTSTRAP = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
final string MONGO_HOST = env("MONGO_HOST", "mongodb");
final int MONGO_PORT = envInt("MONGO_PORT", 27017);
final string MONGO_USER = env("MONGO_USER", "root");
final string MONGO_PASSWORD = env("MONGO_PASSWORD", "dsa612s");
final string MONGO_DB = env("MONGO_DB", "adminreports");
final int SERVICE_PORT = envInt("SERVICE_PORT", 8087);
