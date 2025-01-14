package org.zeplinko.chrono.poc;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.zeplinko.chrono.poc.enums.ProcessCommand;
import org.zeplinko.chrono.poc.services.*;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

@SpringBootApplication
@Slf4j
public class SchedulerApplication {

    public static void main(String[] args) {
        SpringApplication.run(SchedulerApplication.class, args);
    }

    @Bean
    public CommandLineRunner commandLineRunner(
            TriggerCreationService triggerCreationService,
                                               BatchTriggerService batchTriggerService,
                                               SingleTriggerService singleTriggerService,
                                               TransactionalBatchTriggerService transactionalBatchTriggerService,
                                               TransactionalSingleTriggerService transactionalSingleTriggerService) {
        return args -> {
            String commands = Arrays.stream(ProcessCommand.values()).map(ProcessCommand::getCommandName).collect(Collectors.joining());
            if (args.length == 0) {
                log.error("Please specify which script to run: {}", commands);
                System.exit(1);
                return;
            }

            Optional<ProcessCommand> processCommandOptional = ProcessCommand.fromCommandName(args[0]);
            if(processCommandOptional.isEmpty()) {
                log.error("Unknown script: {}", args[0]);
                log.error("Available options: {}", commands);
                System.exit(1);
                return;
            }
            switch (processCommandOptional.get()) {
                case ProcessCommand.CREATE_TRIGGER:
                    log.info("Starting scheduled trigger creation...");
                    triggerCreationService.createTriggers(60, 100, 300);
                    break;

                case ProcessCommand.PROCESS_BATCH_TRIGGER:
                    log.info("Starting Batch processing trigger...");
                    batchTriggerService.processBatchTriggerInOneQuery();
                    break;

                case ProcessCommand.PROCESS_SINGLE_TRIGGER:
                    log.info("Starting Single processing trigger...");
                    singleTriggerService.processSingleTriggerInOneQuery();
                    break;

                case ProcessCommand.PROCESS_TRANSACTIONAL_BATCH_TRIGGER:
                    log.info("Starting Transactional Batch processing trigger...");
                    transactionalBatchTriggerService.processBatchTriggerInTwoQuery();
                    break;

                case ProcessCommand.PROCESS_TRANSACTIONAL_SINGLE_TRIGGER:
                    log.info("Starting Transactional Single processing trigger...");
                    transactionalSingleTriggerService.processSingleTriggerInTwoQuery();
                    break;
            }
        };
    }

}
