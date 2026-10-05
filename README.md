# Distributed Food Delivery Platform

DSA612S — Distributed Systems and Applications, Assignment 2.
A microservices platform built in Ballerina with Kafka for event-driven
coordination, MongoDB for persistence, and Docker Compose for orchestration.

## What's in here

The platform runs as 16 packages. Each logical service has a REST API
and one or more Kafka consumer processes.

| Package | Type | Port |
|---|---|---|
| customer-service | REST API | 8081 |
| customer-consumer | Kafka consumer | — |
| restaurant-service | REST API | 8082 |
| restaurant-consumer | Kafka consumer | — |
| order-service | REST API | 8083 |
| order-consumer | Kafka consumer | — |
| payment-service | REST API | 8084 |
| payment-consumer | Kafka consumer | — |
| delivery-service | REST API | 8085 |
| delivery-consumer | Kafka consumer | — |
| notification-service | REST API | 8086 |
| notification-consumer | Kafka consumer | — |
| admin-service | REST API | 8087 |
| admin-consumer | Kafka consumer | — |
| admin-consumer-kitchen | Kafka consumer | — |
| admin-consumer-delivery | Kafka consumer | — |

The REST and consumer packages for the same service share code, the
same MongoDB database, and the same event contract. They run as
separate processes because Ballerina's scheduler cannot serve HTTP
while consumer poll loops hold strands.

## Running the stack

```bash
git clone https://github.com/JADev9/Food-delivery-platform-DSAassigment2.git
cd Food-delivery-platform-DSAassigment2
docker compose up -d --build
