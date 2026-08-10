package com.evn.billing.worker.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.util.backoff.FixedBackOff;

import org.springframework.beans.factory.annotation.Value;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import java.util.HashMap;
import java.util.Map;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.listener.ContainerProperties;

import org.springframework.context.annotation.Primary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    @Value("${spring.kafka.listener.concurrency:4}")
    private int concurrency;

    @Bean
    @Primary
    public ConsumerFactory<String, Object> consumerFactory(KafkaProperties kafkaProperties) {
        return new DefaultKafkaConsumerFactory<>(kafkaProperties.buildConsumerProperties(null));
    }

    /**
     * Custom container factory that runs consumer poll loops and message processing
     * on Java 21 Virtual Threads, maximizing resource utilization.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        
        SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor("kafka-vt-");
        tryEnableVirtualThreads(executor);
        factory.getContainerProperties().setListenerTaskExecutor(executor);
        
        // Manual offset committing to align with transactional writes
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaBatchListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(true);
        factory.setConcurrency(concurrency);
        
        SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor("kafka-batch-vt-");
        tryEnableVirtualThreads(executor);
        factory.getContainerProperties().setListenerTaskExecutor(executor);
        
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        
        return factory;
    }

    /**
     * Default fallback error handler when message retries fail.
     */
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> template) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
        return new DefaultErrorHandler(recoverer, new FixedBackOff(2000L, 3L));
    }

    /**
     * Dedicated ConsumerFactory cho billing-operations-topic với StringDeserializer.
     * Tách biệt hoàn toàn với global JsonDeserializer của billing-execution-topic.
     */
    @Bean
    public ConsumerFactory<String, String> operationsConsumerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties(null));
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // Xóa các spring.json.* để tránh JsonDeserializer override trở lại
        props.remove("spring.json.value.default.type");
        props.remove("spring.json.use.type.headers");
        props.remove("spring.json.trusted.packages");
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * Dedicated ContainerFactory cho operations topic.
     * AckMode.RECORD: auto-ack sau mỗi message — phù hợp vì listener không nhận Acknowledgment param.
     * @RetryableTopic tự kế thừa factory này theo Spring Kafka 3.x default behavior.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> operationsKafkaListenerContainerFactory(
            @Qualifier("operationsConsumerFactory") ConsumerFactory<String, String> operationsConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(operationsConsumerFactory);

        SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor("kafka-ops-vt-");
        tryEnableVirtualThreads(executor);
        factory.getContainerProperties().setListenerTaskExecutor(executor);

        // RECORD: auto-ack mỗi message thành công — không cần Acknowledgment param trong listener
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);

        return factory;
    }

    private void tryEnableVirtualThreads(SimpleAsyncTaskExecutor executor) {
        try {
            executor.setVirtualThreads(true);
        } catch (UnsupportedOperationException e) {
            log.warn("Virtual threads are not supported on this JDK version ({}). Falling back to standard threads.",
                    System.getProperty("java.version"));
        }
    }
}
