package com.example.demo;

import com.example.demo.base.AbstractKafkaMessageProcessingTestBase;
import com.example.demo.batchListenerManuelRetry.BatchListenerManualRetry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.test.context.ActiveProfiles;

/**
 * Tests for {@link BatchListenerManualRetry} implementation.
 * <p>
 * This implementation uses:
 * <ul>
 *   <li>Batch-level message processing</li>
 *   <li>Manual retry logic within batch</li>
 *   <li>Manual acknowledgment mode</li>
 * </ul>
 */
@Slf4j
@ActiveProfiles("batchListenerManuelRetry")
class BatchListenerManualRetryTest extends AbstractKafkaMessageProcessingTestBase {
}
