package org.zeplinko.chrono.poc.services;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

@Service
@Slf4j
public class TriggerCreationService {

    private final JdbcTemplate jdbcTemplate;

    private final ExecutorService executorService;

    // TODO: Should be a configuration
    private static final int MAX_THREADS = 4;

    public TriggerCreationService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.executorService = Executors.newFixedThreadPool(MAX_THREADS);
    }

    public void createTriggers(int initialDelayInSeconds, int triggersPerSecond, int workloadTimeInSeconds) {
        long firstTriggerTime = Instant.now().toEpochMilli() + initialDelayInSeconds * 1000L;
        List<Long> timestampList = LongStream.range(0, workloadTimeInSeconds)
                .map(timeInSeconds -> firstTriggerTime + timeInSeconds * 1000L)
                .boxed()
                .toList();

        for (int i = 0; i < MAX_THREADS; i++) {
            int finalI = i;
            Runnable runnable = () -> {
                for (int j = finalI; j < timestampList.size(); j += MAX_THREADS) {
                    Long timestamp = timestampList.get(j);
                    createTrigger(timestamp, triggersPerSecond);
                }
            };
            executorService.submit(runnable);
        }
    }

    private void createTrigger(long timestamp, int triggersPerSecond) {
        @SuppressWarnings("SqlNoDataSourceInspection")
        String sql = "INSERT INTO trigger_table (name, version, next_fire_time, status) VALUES (?, ?, ?, ?)";
        List<Object[]> batchParams = IntStream.range(0, triggersPerSecond)
                .mapToObj(i -> new Object[] {
                        "TRIGGER_" + timestamp + "_" + i,
                        timestamp,
                        "PENDING"
                }).toList();
        jdbcTemplate.batchUpdate(sql, batchParams);
    }
}
