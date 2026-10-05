FROM ballerina/ballerina:2201.13.5 AS build
WORKDIR /home/ballerina/app
COPY --chown=ballerina:ballerina . .
RUN bal build

FROM eclipse-temurin:17-jre-jammy
RUN useradd -r -u 10001 -m appuser
WORKDIR /app
COPY --from=build /home/ballerina/app/target/bin/admin_consumer_delivery.jar /app/service.jar
USER appuser
ENTRYPOINT ["java", "-jar", "/app/service.jar"]
