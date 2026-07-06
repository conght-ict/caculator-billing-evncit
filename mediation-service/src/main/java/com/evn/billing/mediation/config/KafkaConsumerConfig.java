package com.evn.billing.mediation.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

@Configuration
public class KafkaConsumerConfig {

    /**
     * Configures the Kafka listener container factory to use Virtual Threads (Loom)
     * for asynchronous message processing in the mediation service.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        
        SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor("mediation-vt-");
        executor.setVirtualThreads(true);
        factory.getContainerProperties().setListenerTaskExecutor(executor);
        
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaBatchListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(true);
        factory.setConcurrency(32);
        
        SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor("mediation-batch-vt-");
        executor.setVirtualThreads(true);
        factory.getContainerProperties().setListenerTaskExecutor(executor);
        
        return factory;
    }

    @Bean
    public org.apache.kafka.clients.admin.NewTopic meterReadingsInputTopic() {
        return org.springframework.kafka.config.TopicBuilder.name("meter-readings-input")
                .partitions(32)
                .replicas(1)
                .build();
    }
}
