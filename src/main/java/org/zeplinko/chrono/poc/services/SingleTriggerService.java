package org.zeplinko.chrono.poc.services;

import org.zeplinko.chrono.poc.constants.Constant;
import org.zeplinko.chrono.poc.utils.KafkaUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
public class SingleTriggerService {

    public static final String SQL = "WITH cte AS (SELECT id, version FROM trigger_table WHERE status = 'PENDING' ORDER BY id LIMIT 1) " +
            "UPDATE trigger_table SET status = 'ACQUIRED', version = cte.version + 1 FROM cte WHERE trigger_table.id = cte.id RETURNING trigger_table.id;";

    private final KafkaUtils kafkaUtils;

    private final JdbcTemplate jdbcTemplate;

    public SingleTriggerService(KafkaUtils kafkaUtils, JdbcTemplate jdbcTemplate) {
        this.kafkaUtils = kafkaUtils;
        this.jdbcTemplate = jdbcTemplate;
    }

    public void processSingleTriggerInOneQuery() {
        while (true) {
            try {
                log.info("Processing trigger in one query");
                List<Long> updatedTriggerId = jdbcTemplate.queryForList(SQL, Long.class);
                if (!updatedTriggerId.isEmpty()) {
                    kafkaUtils.sendToKafka(updatedTriggerId.getFirst(), Constant.KAFKA_TOPIC);
                    log.info("Updated record with ID: {}", updatedTriggerId);
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
