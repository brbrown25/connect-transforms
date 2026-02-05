.PHONY: help build clean test integration-test all-tests coverage coverage-all coverage-verify lint lint-fix jar plugin-jar publish docker run-docker

# Default target
help:
	@echo "Kafka Connect Transforms - Available targets:"
	@echo ""
	@echo "  build              Build the project (skip tests)"
	@echo "  clean              Clean build artifacts"
	@echo "  test               Run unit tests"
	@echo "  integration-test   Run integration tests (requires Docker)"
	@echo "  all-tests          Run all tests (unit + integration)"
	@echo "  coverage           Generate unit test coverage report"
	@echo "  coverage-all       Generate combined coverage report (unit + integration)"
	@echo "  coverage-verify    Run tests and verify coverage thresholds"
	@echo "  lint               Run checkstyle"
	@echo "  lint-fix           Auto-fix formatting issues with Spotless"
	@echo "  jar                Build the main JAR"
	@echo "  plugin-jar         Build the Connect plugin fat JAR"
	@echo "  publish            Publish to GitHub Packages (requires credentials)"
	@echo "  docker             Build Docker image"
	@echo "  run-docker         Run Kafka Connect with plugin in Docker"
	@echo ""

# Build without tests
build:
	./gradlew build -x test -x integrationTest -x checkstyleIntegrationTest -x jacocoTestCoverageVerification

# Clean build artifacts
clean:
	./gradlew clean

# Run unit tests
test:
	./gradlew test

# Run integration tests (requires Docker)
integration-test:
	./gradlew integrationTest

# Run all tests
all-tests:
	./gradlew test integrationTest --continue

# Generate unit test coverage report
coverage:
	./gradlew test jacocoTestReport
	@echo ""
	@echo "Coverage report: build/reports/jacoco/test/html/index.html"

# Generate combined coverage report (unit + integration tests)
coverage-all:
	./gradlew test integrationTest jacocoCombinedReport --continue
	@echo ""
	@echo "Combined coverage report: build/reports/jacoco/jacocoCombinedReport/html/index.html"

# Run tests and verify coverage thresholds
coverage-verify:
	./gradlew test jacocoTestCoverageVerification

# Run checkstyle and spotless formatting checks
lint:
	./gradlew checkstyleMain checkstyleTest checkstyleIntegrationTest spotlessCheck

# Auto-fix formatting issues
lint-fix:
	./gradlew spotlessApply

# Build main JAR
jar:
	./gradlew jar

# Build Connect plugin fat JAR
plugin-jar:
	./gradlew connectPluginJar
	@echo ""
	@echo "Plugin JAR: build/libs/*-connect-plugin.jar"

# Publish to GitHub Packages
publish:
	./gradlew publish

# Build Docker image
docker:
	docker build -t connect-transforms:latest .

# Run Kafka Connect with plugin in Docker (for local testing)
run-docker: docker
	docker run --rm -it \
		-p 8083:8083 \
		-e CONNECT_BOOTSTRAP_SERVERS=localhost:9092 \
		-e CONNECT_REST_PORT=8083 \
		-e CONNECT_GROUP_ID=connect-cluster \
		-e CONNECT_CONFIG_STORAGE_TOPIC=connect-configs \
		-e CONNECT_OFFSET_STORAGE_TOPIC=connect-offsets \
		-e CONNECT_STATUS_STORAGE_TOPIC=connect-status \
		-e CONNECT_KEY_CONVERTER=org.apache.kafka.connect.storage.StringConverter \
		-e CONNECT_VALUE_CONVERTER=org.apache.kafka.connect.storage.StringConverter \
		connect-transforms:latest

# Full CI build (what runs in GitHub Actions)
ci: lint test coverage-verify

# Release build
release: clean lint all-tests plugin-jar
	@echo ""
	@echo "Release artifacts ready in build/libs/"
