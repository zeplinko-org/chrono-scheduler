package org.zeplinko.chrono.poc.models;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "trigger")
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class Trigger {

    @Id
    private String id;
    private Long triggerId;

    public Trigger(Long triggerId) {
        this.triggerId = triggerId;
    }
}
