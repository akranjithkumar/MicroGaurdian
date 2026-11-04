FROM eclipse-temurin:17-jre-alpine
LABEL maintainer="microguardian"

WORKDIR /app
COPY target/*.jar app.jar

EXPOSE 8081

ENTRYPOINT ["java", "-jar", "app.jar"]
