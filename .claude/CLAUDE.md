# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Spring Boot 3.5 demonstration project showing how to handle Kafka JSON deserialization errors using Spring Kafka's `ErrorHandlingDeserializer`. The core purpose is to demonstrate that when invalid JSON arrives on a Kafka topic, the consumer's message handler is still invoked (with a null value) allowing the application to handle deserialization failures gracefully.

**Key Architecture Pattern:**
- Kafka messages are consumed as `DocumentOperation` records (JSON deserialized)
- The `ErrorHandlingDeserializer` wraps the `JsonDeserializer` so deserialization failures don't crash the consumer
- When deserialization fails, the message listener receives a `ConsumerRecord` with `null` value
- The application extracts the exception from Kafka headers using `SerializationUtils.getExceptionFromHeader()`
- This allows tracking both successful and failed messages

## Build & Run Commands

**Build the project:**
```bash
./mvnw clean install
```

**Run tests:**
```bash
./mvnw test
```

**Run a single test:**
```bash
./mvnw test -Dtest=KafkaDeserializationTest
```

**Run the application:**
```bash
./mvnw spring-boot:run
```

## Architecture Details

### Kafka Configuration Flow

The Kafka configuration uses a specific setup in `KafkaConfig.java`:

1. Creates a `JsonDeserializer<DocumentOperation>` configured with trusted packages
2. Wraps it with `ErrorHandlingDeserializer` for both key and value deserializers
3. This ensures deserialization exceptions are captured in Kafka message headers rather than crashing the consumer

### Message Processing Flow

The message flow through `KafkaInputChannel.java`:

1. `ConcurrentMessageListenerContainer` receives messages from the "input-topic"
2. `handleRecord()` method checks if `data.value()` is null
3. If null, extracts the deserialization exception from headers
4. Maintains counters: `processedCount` for successful messages, `errorCount` for deserialization failures

**Important:** The commented line `containerProperties.setCheckDeserExWhenValueNull(false)` in `KafkaInputChannel.java:33` represents a configuration that was explored during development. The current behavior works without this setting.

### Testing Strategy

`KafkaDeserializationTest` uses:
- `@EmbeddedKafka` for integration testing without external Kafka
- Sends both valid JSON and invalid raw text to verify both paths
- Uses `Awaitility` to wait for async message processing
- Verifies counters to confirm both successful and failed message handling

## Technology Stack

- **Java 21** (required)
- **Spring Boot 3.5.0** with Spring Kafka
- **Maven** (wrapper included: `mvnw`/`mvnw.cmd`)
- **Lombok** for code generation (@Slf4j, etc.)
- **Embedded Kafka** for testing (no external Kafka needed for tests)

## Windows Development Notes

This project uses Maven wrapper scripts. On Windows:
- Use `mvnw.cmd` for Command Prompt
- Use `./mvnw` for Git Bash/MINGW64

# Code Style & Working Preferences

## General Formatting
- Use indentation and formatting consistent with existing project settings
- Always include meaningful comments for complex or non-obvious logic
- Prefer descriptive variable and method names over short abbreviations
- Keep line length reasonable (max 160 characters)

## Java / Spring Boot
- Use Lombok annotations where appropriate (`@Data`, `@Builder`, `@Slf4j`, etc.)
- Follow Spring dependency injection patterns
- Prefer constructor injection over field injection (avoid field injection)
- Include JavaDoc for public APIs and public library-facing classes/methods

---

## Testing
- Write unit tests for all public methods
- Use descriptive test method names, such as `shouldDoSomething_WhenCondition_ThenExpectedOutcome`
- Include both positive and negative test cases
- Use embedded Kafka for integration tests when testing pipeline workers or Kafka-related code

### Test Execution & Logs
- Run test suites only once per context (avoid repeated executions of commands like `./mvnw test` with different `grep` filters).
- When running Maven tests, follow this pattern for log handling:
  1. Use the appropriate Maven command for the situation (e.g. `./mvnw test`, `./mvnw clean test`, module-specific targets, etc.).
  2. Ensure a dedicated log directory exists outside of `target`, for example `.ai/.logs/` in the project root.
    - Example: `mkdir -p .ai/logs`
  3. Redirect the full output of the Maven command to a log file inside this directory.
    - Example: `./mvnw test > .ai/logs/mvn-test.log 2>&1`
  4. Perform all log analysis only on the stored log file (`grep`, `rg`, `sed`, etc.).
    - Example: `grep "some pattern" .ai/logs/mvn-test.log`
- If tests must be rerun due to code changes, overwrite the log file.
  - Example: `./mvnw test > .ai/logs/mvn-test.log 2>&1`
- If tests should be started multiple times in parallel (for example, to compare the results), add a meaningful suffix to the log file name, such as `.ai/logs/mvn-test-theory-1.log`.
