package org.zeplinko.chrono.poc.services;

import org.zeplinko.chrono.poc.constants.Constant;
import org.zeplinko.chrono.poc.utils.KafkaUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Slf4j
public class TransactionalSingleTriggerService {

    public static final String SELECT_ID_FROM_TRIGGER_TABLE_WHERE_STATUS_PENDING_ORDER_BY_ID_LIMIT_1 = "SELECT id FROM trigger_table WHERE status = 'PENDING' ORDER BY id LIMIT 1";
    public static final String UPDATE_TRIGGER_TABLE_SET_STATUS_ACQUIRED_WHERE_ID = "UPDATE trigger_table SET status = 'ACQUIRED', version = version + 1 WHERE id = ?";
    private final KafkaUtils kafkaUtils;

    private final JdbcTemplate jdbcTemplate;

    public TransactionalSingleTriggerService(KafkaUtils kafkaUtils, JdbcTemplate jdbcTemplate) {
        this.kafkaUtils = kafkaUtils;
        this.jdbcTemplate = jdbcTemplate;
    }

    public void processSingleTriggerInTwoQuery() {
        while (true) {
            try {
                log.info("Starting transactional trigger processing");
                boolean recordsFound = processSingleRecord();
                if (!recordsFound) {
                    log.info("No pending records found. Waiting for 1 minute...");
                    Thread.sleep(60000);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Thread interrupted. Stopping process.");
                break;
            } catch (Exception ex) {
                log.error("Error occurred while processing record: {}", ex.getMessage(), ex);
            }
        }
    }

    @Transactional
    protected boolean processSingleRecord() {
        Long pendingId = jdbcTemplate.queryForObject(
                SELECT_ID_FROM_TRIGGER_TABLE_WHERE_STATUS_PENDING_ORDER_BY_ID_LIMIT_1,
                Long.class
        );
        if (pendingId == null) {
            return false;
        }
        int rowsUpdated = jdbcTemplate.update(
                UPDATE_TRIGGER_TABLE_SET_STATUS_ACQUIRED_WHERE_ID,
                pendingId
        );
        if (rowsUpdated > 0) {
            kafkaUtils.sendToKafka(pendingId, Constant.KAFKA_TOPIC);
            log.info("Updated record with ID: {}", pendingId);
        }
        return true;
    }

}
