package org.zeplinko.chrono.poc.services;

import org.zeplinko.chrono.poc.models.Trigger;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
public class MongoService {

    public static final String NO_TRIGGER_IDS_TO_SAVE = "No trigger IDs to save.";

    private final MongoTemplate mongoTemplate;

    public MongoService(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    private static boolean checkIfTriggerIsEmpty(List<Long> triggerIdsList) {
        if (triggerIdsList.isEmpty()) {
            log.info(NO_TRIGGER_IDS_TO_SAVE);
            return true;
        }
        return false;
    }

    public void saveTriggerIds(List<Long> triggerIdsList) {
        if (checkIfTriggerIsEmpty(triggerIdsList)) return;

        List<Trigger> documents = triggerIdsList.stream()
                .map(Trigger::new)
                .collect(Collectors.toList());
        mongoTemplate.insertAll(documents);
        log.info("Saved {} trigger IDs to MongoDB.", documents.size());
    }
}
