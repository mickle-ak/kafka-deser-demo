package com.example.demo;

import com.example.demo.base.AbstractKafkaMessageProcessingTestBase;
import lombok.extern.slf4j.Slf4j;
import org.springframework.test.context.ActiveProfiles;

@Slf4j
@ActiveProfiles("recordListenerCustomErrorHandler")
class RecordListenerCustomErrorHandlerTest extends AbstractKafkaMessageProcessingTestBase {
}
