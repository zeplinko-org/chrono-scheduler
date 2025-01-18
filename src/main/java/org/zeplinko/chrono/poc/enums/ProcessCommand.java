package org.zeplinko.chrono.poc.enums;

import lombok.Getter;

import java.util.Optional;

@Getter
public enum ProcessCommand {
    CREATE_TRIGGER("create-trigger"),
    PROCESS_BATCH_TRIGGER("process-batch-trigger"),
    PROCESS_SINGLE_TRIGGER("process-single-trigger"),
    PROCESS_TRANSACTIONAL_BATCH_TRIGGER("process-transactional-batch-trigger"),
    PROCESS_TRANSACTIONAL_SINGLE_TRIGGER("process-transactional-single-trigger"),
    ;

    private final String commandName;

    ProcessCommand(String commandName) {
        this.commandName = commandName;
    }

    public static Optional<ProcessCommand> fromCommandName(String commandName) {
        try {
            return Optional.of(ProcessCommand.valueOf(commandName));
        } catch (Exception e) {
            return Optional.empty();
        }
    }
}
