FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY . .
RUN mvn -B -q -DskipTests package

FROM eclipse-temurin:17-jre
ARG MODULE
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /src/${MODULE}/target/app.jar app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
