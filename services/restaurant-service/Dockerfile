FROM ballerina/ballerina:2201.13.5 AS build
WORKDIR /home/ballerina/app
COPY --chown=ballerina:ballerina . .
RUN bal build

FROM eclipse-temurin:17-jre-jammy
RUN useradd -r -u 10001 -m appuser
WORKDIR /app
COPY --from=build /home/ballerina/app/target/bin/restaurant_service.jar /app/service.jar
USER appuser
EXPOSE 8082
ENTRYPOINT ["java", "-jar", "/app/service.jar"]
