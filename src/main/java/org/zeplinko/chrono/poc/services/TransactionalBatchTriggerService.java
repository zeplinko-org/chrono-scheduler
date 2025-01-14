package org.zeplinko.chrono.poc.services;

import org.zeplinko.chrono.poc.constants.Constant;
import org.zeplinko.chrono.poc.utils.KafkaUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
public class TransactionalBatchTriggerService {
    public static final String SELECT_ID_FROM_TRIGGER_TABLE_WHERE_STATUS_PENDING_ORDER_BY_ID_LIMIT = "SELECT id FROM trigger_table WHERE status = 'PENDING' ORDER BY id LIMIT ?";
    private static final int BATCH_SIZE = 5;
    private final KafkaUtils kafkaUtils;

    private final JdbcTemplate jdbcTemplate;

    public TransactionalBatchTriggerService(KafkaUtils kafkaUtils, JdbcTemplate jdbcTemplate) {
        this.kafkaUtils = kafkaUtils;
        this.jdbcTemplate = jdbcTemplate;
    }

    public void processBatchTriggerInTwoQuery() {
        while (true) {
            try {
                log.info("Starting transactional batch trigger");
                boolean recordsFound = processSingleBatch();
                if (!recordsFound) {
                    log.info("No pending records found. Sleeping for 1 minute...");
                    Thread.sleep(60000);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Thread interrupted. Stopping process.");
                break;
            } catch (Exception ex) {
                log.error("Error occurred while processing batch: {}", ex.getMessage(), ex);
            }
        }
    }

    @Transactional
    protected boolean processSingleBatch() {
        List<Long> pendingTriggerIds = jdbcTemplate.queryForList(
                SELECT_ID_FROM_TRIGGER_TABLE_WHERE_STATUS_PENDING_ORDER_BY_ID_LIMIT,
                Long.class,
                BATCH_SIZE
        );
        if (pendingTriggerIds.isEmpty()) {
            log.info("No pending records found.");
            return false;
        }
        String idList = pendingTriggerIds.stream()
                .map(String::valueOf)
                .collect(Collectors.joining(","));
        int rowsUpdated = jdbcTemplate.update(
                "UPDATE trigger_table SET status = 'ACQUIRED', version = version + 1 WHERE id IN (" + idList + ")"
        );
        log.info("Rows updated: {}", rowsUpdated);
        if (rowsUpdated > 0) {
            kafkaUtils.sendToKafka(pendingTriggerIds, Constant.KAFKA_TOPIC);
        }
        return true;
    }

}
