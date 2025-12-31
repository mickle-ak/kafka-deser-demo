# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Spring Boot 3.5 demonstration project showing how to handle Kafka JSON deserialization errors and processing errors using Spring Kafka's `ErrorHandlingDeserializer` and `DefaultErrorHandler`. The core purpose is to demonstrate:
1. Graceful handling of deserialization errors (invalid JSON) without crashing the consumer
2. Retry logic for transient processing errors with configurable backoff strategy
3. Distinction between retryable and non-retryable errors

**Key Architecture Patterns:**

### Two-Tier Error Handling Strategy

**1. Deserialization Errors (Non-Retryable)**
- Handled by `ErrorHandlingDeserializer` wrapping `JsonDeserializer`
- When deserialization fails, the batch listener receives a `ConsumerRecord` with `null` value
- Exception extracted from Kafka headers using `SerializationUtils.getExceptionFromHeader()`
- These errors are NOT retried (data is corrupted)
- Logged and counted, then skipped to process next message

**2. Processing Errors (Potentially Retryable)**
- Handled by `DefaultErrorHandler` configured in `ErrorHandlerConfig.java`
- Retry strategy: 3 attempts with 500ms fixed backoff
- `RetryableException` → triggers retry
- `NotRetryableException` & `IllegalArgumentException` → no retry
- Failed messages after all retries → sent to `ConsumerRecordRecoverer` (currently logging only, should use DLQ in production)

### Message Processing Model
- Uses **batch-level listening** (`BatchAcknowledgingMessageListener`) instead of record-level
- **Manual acknowledgment mode** (`AckMode.MANUAL`) for at-least-once semantics
- Batch is acknowledged only after all records are processed
- If processing fails before acknowledgment, offset is not committed → redelivery occurs

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

### Component Overview

**KafkaConfig.java** - Kafka consumer configuration
1. Creates a `JsonDeserializer<DocumentOperation>` configured with trusted packages
2. Wraps it with `ErrorHandlingDeserializer` for both key and value deserializers
3. This ensures deserialization exceptions are captured in Kafka message headers rather than crashing the consumer

**ErrorHandlerConfig.java** - Error handling and retry configuration (see `ErrorHandlerConfig.java`)
1. **pipelineProcessingConsumerErrorHandler** - Configures `DefaultErrorHandler` with:
   - Retry strategy (3 attempts, 500ms backoff)
   - Retryable exceptions: `RetryableException`
   - Non-retryable exceptions: `NotRetryableException`, `IllegalArgumentException`
   - Manual acknowledgment settings: `setAckAfterHandle(true)`, `setCommitRecovered(true)`
2. **consumerRecordRecoverer** - `LoggingOnlyRecoverer` logs failed records after all retries
   - ⚠️ Production should use `DeadLetterPublishingRecoverer` to send to DLQ
3. **loggingRetryListener** - Logs each retry attempt with delivery attempt number

**KafkaInputChannel.java** - Message listener and processing (see `KafkaInputChannel.java`)
1. Uses `BatchAcknowledgingMessageListener` for batch-level message processing
2. Configured with manual acknowledgment mode (`AckMode.MANUAL`)
3. Injects `CommonErrorHandler` from `ErrorHandlerConfig`
4. Processes each record in batch, separating deserialization errors from successful messages

### Message Processing Flow

**Normal Flow (KafkaInputChannel.java:54-71):**
1. `ConcurrentMessageListenerContainer` receives batch of messages from "input-topic"
2. `handleBatch()` iterates through each `ConsumerRecord` in the batch
3. For each record:
   - **If `data.value() != null`** → `processRecord()` → increment `processedCount`
   - **If `data.value() == null`** → `processDeserializationError()` → extract exception from headers → log error → increment `errorCount`
4. After all records processed → `acknowledgment.acknowledge()` commits offsets

**Error Handling Flow:**
- **Deserialization errors**: Handled inline in `handleBatch()`, no retry, continue to next record
- **Processing errors**: If `processRecord()` throws:
  - `RetryableException` → `DefaultErrorHandler` retries up to 3 times with 500ms backoff
  - `NotRetryableException` or `IllegalArgumentException` → no retry, sent to recoverer immediately
  - Other exceptions → follows retry policy based on handler configuration

### Key Implementation Details

- **Batch listening is crucial**: With batch listeners, null values from deserialization errors ARE passed to the listener (unlike record-level listeners)
- **Manual acknowledgment**: Ensures at-least-once semantics - if processing fails before `acknowledge()`, messages are redelivered
- **Error handler settings**: `setAckAfterHandle(true)` and `setCommitRecovered(true)` are important for manual acknowledge mode

### Custom Exception Classes

**ErrorHandlerConfig.RetryableException** (see `ErrorHandlerConfig.java:186-202`)
- Use this for transient errors that should be retried (network issues, temporary service unavailability, etc.)
- Triggers the retry mechanism with backoff strategy

**ErrorHandlerConfig.NotRetryableException** (see `ErrorHandlerConfig.java:165-182`)
- Use this for permanent errors that won't succeed on retry (validation errors, business logic violations, etc.)
- Skips retry and goes directly to the recoverer

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
