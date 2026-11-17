# syntax=docker/dockerfile:1
FROM maven:3.9.16-eclipse-temurin-21 AS build
WORKDIR /workspace/audit-service
COPY audit-service/pom.xml ./pom.xml
COPY audit-service/src ./src
RUN mvn -B -DskipTests package \
    org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy-dependencies \
    -DincludeScope=runtime -DoutputDirectory=target/dependency \
    && java -cp target/classes:target/test-classes org.g12.audit.AuditProtocolSelfTest

FROM eclipse-temurin:21-jre AS runtime
WORKDIR /app
USER 10001:10001
COPY --from=build --chown=10001:10001 /workspace/audit-service/target/g12-audit-service-1.0-SNAPSHOT.jar /app/app.jar
COPY --from=build --chown=10001:10001 /workspace/audit-service/target/dependency/ /app/dependency/
ENTRYPOINT ["java", "-cp", "/app/app.jar:/app/dependency/*"]
CMD ["org.g12.audit.AuditApplication"]
