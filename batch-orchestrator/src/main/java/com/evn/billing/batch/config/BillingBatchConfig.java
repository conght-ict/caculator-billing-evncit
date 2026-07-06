package com.evn.billing.batch.config;

import com.evn.billing.common.domain.Account;
import com.evn.billing.batch.dto.BillingTaskDto;
import jakarta.persistence.EntityManagerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.batch.item.kafka.KafkaItemWriter;
import org.springframework.batch.item.kafka.builder.KafkaItemWriterBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import com.evn.billing.batch.listener.BillingJobListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import java.util.Map;
import java.util.UUID;

@Configuration
public class BillingBatchConfig {

    @Autowired
    private EntityManagerFactory entityManagerFactory;

    @Autowired
    private KafkaTemplate<String, BillingTaskDto> kafkaTemplate;

    /**
     * Reads active Accounts belonging to a specific Book_ID page-by-page.
     */
    @Bean
    @StepScope
    public JpaPagingItemReader<Account> reader(
            @Value("#{jobParameters['bookId']}") String bookId) {
        return new JpaPagingItemReaderBuilder<Account>()
                .name("accountReader")
                .entityManagerFactory(entityManagerFactory)
                .queryString("SELECT a FROM Account a WHERE a.bookId = :bookId AND a.status = 'ACTIVE'")
                .parameterValues(Map.of("bookId", bookId))
                .pageSize(1000)
                .build();
    }

    /**
     * Maps Account entity to Kafka execution task, adding unique tracing and version metrics.
     */
    @Bean
    @StepScope
    public ItemProcessor<Account, BillingTaskDto> processor(
            @Value("#{jobParameters['month']}") String month,
            @Value("#{jobParameters['calculationVersion']}") Long version) {
        return account -> {
            String traceId = UUID.randomUUID().toString().replace("-", "");
            int effectiveVersion = version != null ? version.intValue() : 1;
            return new BillingTaskDto(
                    account.getAccountId(),
                    account.getBookId(),
                    month,
                    effectiveVersion,
                    traceId
            );
        };
    }

    /**
     * Writes processed task chunks directly into Apache Kafka.
     * Kafka Partition Key is set to AccountId to preserve routing.
     */
    @Bean
    public KafkaItemWriter<String, BillingTaskDto> writer() {
        return new KafkaItemWriterBuilder<String, BillingTaskDto>()
                .kafkaTemplate(kafkaTemplate)
                .itemKeyMapper(BillingTaskDto::getAccountId)
                .build();
    }

    /**
     * Defines Step with Chunk size of 1000 accounts.
     */
    @Bean
    public Step billingStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                            JpaPagingItemReader<Account> reader, ItemProcessor<Account, BillingTaskDto> processor,
                            KafkaItemWriter<String, BillingTaskDto> writer) {
        return new StepBuilder("billingStep", jobRepository)
                .<Account, BillingTaskDto>chunk(1000, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    /**
     * Defines Job containing the calculation steps.
     */
    @Bean
    public BillingJobListener billingJobListener() {
        return new BillingJobListener();
    }

    @Bean
    public Job billingJob(JobRepository jobRepository, Step billingStep) {
        return new JobBuilder("billingJob", jobRepository)
                .start(billingStep)
                .listener(billingJobListener())
                .build();
    }
}
