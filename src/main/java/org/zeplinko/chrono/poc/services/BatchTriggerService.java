package org.zeplinko.chrono.poc.services;

import org.zeplinko.chrono.poc.constants.Constant;
import org.zeplinko.chrono.poc.utils.KafkaUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
public class BatchTriggerService {

    public static final String SQL = "WITH cte AS (SELECT id, version FROM trigger_table WHERE status = 'PENDING' ORDER BY id LIMIT ?) " +
            "UPDATE trigger_table SET status = 'ACQUIRED', version = version + 1 WHERE id IN (SELECT id FROM cte) RETURNING id;";

    private static final int BATCH_SIZE = 5;

    private final KafkaUtils kafkaUtils;

    private final JdbcTemplate jdbcTemplate;

    public BatchTriggerService(KafkaUtils kafkaUtils, JdbcTemplate jdbcTemplate) {
        this.kafkaUtils = kafkaUtils;
        this.jdbcTemplate = jdbcTemplate;
    }

    public void processBatchTriggerInOneQuery() {
        while (true) {
            int updatedRows;
            try {
                List<Long> updatedTriggerIdsList = jdbcTemplate.queryForList(SQL, Long.class, BATCH_SIZE);
                updatedRows = updatedTriggerIdsList.size();

                if (!updatedTriggerIdsList.isEmpty()) {
                    kafkaUtils.sendToKafka(updatedTriggerIdsList, Constant.KAFKA_TOPIC);
                    log.info("Updated {} records in batch.", updatedRows);
                } else {
                    log.info("No pending records found. Waiting for 1 minute...");
                    Thread.sleep(60000);
                }
                //TODO: Need to handle condition if data is no pushed in KAFKA.
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Thread interrupted. Stopping process.");
                break;
            } catch (Exception ex) {
                log.error("Error occurred while processing batch: {}", ex.getMessage());
            }
        }
    }

}
