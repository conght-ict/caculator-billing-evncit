package com.evn.billing.batch.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaProducerConfig {

    /**
     * Declares the Kafka execution topic. Spring Kafka Admin will automatically
     * provision this topic on the Kafka broker.
     */
    @Bean
    public NewTopic billingExecutionTopic() {
        return TopicBuilder.name("billing-execution-topic")
                .partitions(32) // Scalable partition number for workers scale-out
                .replicas(1)    // Single replica for default environments
                .build();
    }
}
