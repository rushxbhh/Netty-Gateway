# syntax=docker/dockerfile:1

# Build the gateway and its integration-test client.
FROM maven:3.9.16-eclipse-temurin-21 AS build
WORKDIR /workspace

COPY pom.xml ./
COPY src ./src

# package also compiles src/test/java; the standalone GatewayTestClient is
# used by the integration-test Compose profile (it is not a JUnit test).
RUN mvn -B -DskipTests package \
    org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy-dependencies \
    -DincludeScope=runtime \
    -DoutputDirectory=target/dependency

# Runtime image: only Java 21, the gateway JAR, dependencies, and test client.
FROM eclipse-temurin:21-jre AS runtime
WORKDIR /app

# Use the same numeric UID/GID in the Gateway and Sequencer containers so
# both can access files in the shared /dev/shm IPC namespace.
USER 10001:10001

COPY --from=build --chown=10001:10001 \
    /workspace/target/Netty-Ingress-Gateway-1.0-SNAPSHOT.jar /app/app.jar
COPY --from=build --chown=10001:10001 \
    /workspace/target/dependency/ /app/dependency/
COPY --from=build --chown=10001:10001 \
    /workspace/target/test-classes/ /app/test-classes/

EXPOSE 8080

# Compose supplies the main class for the gateway or integration-test client.
ENTRYPOINT ["java", "-cp", "/app/app.jar:/app/dependency/*:/app/test-classes"]
CMD ["org.java.GatewayServer"]
