FROM openjdk:17-jdk-slim

# Set working directory
WORKDIR /app

# Copy application JAR
COPY target/mysql-kafka-sync-tool-1.0.0.jar app.jar

# Create user for running the application
RUN groupadd -r sync && useradd -r -g sync sync

# Change ownership of the app directory
RUN chown -R sync:sync /app

# Switch to non-root user
USER sync

# Expose application port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD curl -f http://localhost:8080/api/actuator/health || exit 1

# Run the application
ENTRYPOINT ["java", "-jar", "app.jar"]