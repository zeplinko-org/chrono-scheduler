package org.zeplinko.chrono.poc.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

@Component
@Slf4j
public class KafkaUtils {

    private final KafkaTemplate<String, String> kafkaTemplate;

    private final ObjectMapper objectMapper;

    public KafkaUtils(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void sendToKafka(List<Long> triggerIdList, String KAFKA_TOPIC) {
        try {
            String message = objectMapper.writeValueAsString(triggerIdList);
            kafkaTemplate.send(KAFKA_TOPIC, UUID.randomUUID().toString(), message);
            log.info("Sent updated IDs List to Kafka as JSON: {}", message);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void sendToKafka(Long triggerId, String KAFKA_TOPIC) {
        try {
            String message = objectMapper.writeValueAsString(triggerId);
            kafkaTemplate.send(KAFKA_TOPIC, UUID.randomUUID().toString(), message);
            log.info("Sent updated IDs to Kafka as JSON: {}", message);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
