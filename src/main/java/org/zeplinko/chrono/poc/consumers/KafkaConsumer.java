package org.zeplinko.chrono.poc.consumers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.zeplinko.chrono.poc.constants.Constant;
import org.zeplinko.chrono.poc.services.MongoService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
public class KafkaConsumer {

    private final MongoService mongoService;

    private final ObjectMapper objectMapper;

    public KafkaConsumer(MongoService mongoService, ObjectMapper objectMapper) {
        this.mongoService = mongoService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = {Constant.KAFKA_TOPIC})
    public void saveSingleTriggerIds(String value, Acknowledgment acknowledgment) {
        try {
            log.info("Received trigger message from Kafka: {}", value);
            List<Long> triggerIdsList;
            if (value.trim().startsWith("[")) {
                triggerIdsList = objectMapper.readValue(value, new TypeReference<>() {
                });
            } else {
                Long singleId = objectMapper.readValue(value, Long.class);
                triggerIdsList = List.of(singleId);
            }

            mongoService.saveTriggerIds(triggerIdsList);
            log.info("Sent trigger IDs to Kafka as JSON: {}", triggerIdsList);
            acknowledgment.acknowledge();
        } catch (JsonProcessingException e) {
            log.error("Error while processing trigger message: {}", e.getMessage(), e);
        }
    }

}
