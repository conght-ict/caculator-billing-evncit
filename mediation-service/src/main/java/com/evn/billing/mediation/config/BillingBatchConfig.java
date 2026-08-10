package com.evn.billing.mediation.config;

import com.evn.billing.common.domain.Account;
import com.evn.billing.common.dto.BillingTaskDto;
import com.evn.billing.common.dto.MeterReadingDto;
import com.evn.billing.common.dto.BillingConfigSnapshot;
import com.evn.billing.mediation.listener.BillingJobListener;
import com.evn.billing.mediation.listener.CmisIngestionListener;
import com.evn.billing.mediation.repository.MeterUsageRepository;
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
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import java.util.Map;
import java.util.UUID;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;

@Configuration
public class BillingBatchConfig {

    @Autowired
    private EntityManagerFactory entityManagerFactory;

    @Autowired
    private KafkaTemplate<String, BillingTaskDto> kafkaTemplate;

    @Autowired
    private MeterUsageRepository meterUsageRepository;
 
    @Autowired
    private com.evn.billing.mediation.repository.BillInvoiceRepository billInvoiceRepository;

    @Autowired
    @Lazy
    private CmisIngestionListener cmisIngestionListener;

    /**
     * Reads active Accounts belonging to a specific Book_ID page-by-page.
     */
    @Bean
    @StepScope
    public JpaPagingItemReader<Account> accountReader(
            @Value("#{jobParameters['dtuongQly']}") String dtuongQly,
            @Value("#{jobParameters['month']}") String month,
            @Value("#{jobParameters['period']}") Long period) {
        
        Logger log = LoggerFactory.getLogger(BillingBatchConfig.class);
        log.info("Initializing accountReader with dtuongQly: {}, month: {}, period: {}, entityManagerFactory: {}", 
                 dtuongQly, month, period, entityManagerFactory);
                 
        if (entityManagerFactory == null) {
            throw new IllegalStateException("entityManagerFactory is null in BillingBatchConfig!");
        }
        if (dtuongQly == null) {
            throw new IllegalArgumentException("jobParameter 'dtuongQly' is null!");
        }
        if (month == null) {
            throw new IllegalArgumentException("jobParameter 'month' is null!");
        }

        int effectivePeriod = period != null ? period.intValue() : 1;
        
        Map<String, Object> paramValues = new HashMap<>();
        paramValues.put("dtuongQly", dtuongQly);
        paramValues.put("month", month);
        paramValues.put("period", effectivePeriod);

        return new JpaPagingItemReaderBuilder<Account>()
                .name("accountReader")
                .entityManagerFactory(entityManagerFactory)
                .queryString("SELECT DISTINCT a FROM Account a JOIN MeterPoint mp ON mp.maKhang = a.maKhang WHERE mp.dtuongQly = :dtuongQly AND a.trangThai = 'ACTIVE' " +
                             "AND NOT EXISTS (SELECT abs FROM AccountBillingStatus abs " +
                             "WHERE abs.maKhang = a.maKhang AND abs.thangChuKy = :month " +
                             "AND abs.kyChot = :period AND abs.trangThai IN ('SUCCESS', 'SUCCESS_CMIS'))")
                .parameterValues(paramValues)
                .pageSize(1000)
                .build();
    }

    /**
     * Maps Account entity to Kafka execution task, adding unique tracing and version metrics.
     */
    @Bean
    @StepScope
    public ItemProcessor<Account, BillingTaskDto> accountProcessor(
            @Value("#{jobParameters['dtuongQly']}") String dtuongQly,
            @Value("#{jobParameters['month']}") String month,
            @Value("#{jobParameters['period']}") Long period,
            @Value("#{jobParameters['calculationVersion']}") Long version) {
        return account -> {
            String traceId = UUID.randomUUID().toString().replace("-", "");
            int effectivePeriod = period != null ? period.intValue() : 1;
            int nextVersion = (int) billInvoiceRepository.countByMaKhangAndThangChuKyAndKyChot(account.getMaKhang(), month, effectivePeriod) + 1;
            
            List<com.evn.billing.common.domain.MeterUsage> validatedUsages = meterUsageRepository
                    .findByMaKhangAndThangChuKyAndKyChotAndTrangThaiXuLy(account.getMaKhang(), month, effectivePeriod, "VALIDATED");
            
            List<MeterReadingDto> readings = validatedUsages.stream()
                    .map(u -> new MeterReadingDto(
                            u.getMaDdo(),
                            u.getTuNgay(),
                            u.getDenNgay(),
                            u.getChiSoDau(),
                            u.getChiSoCuoi(),
                            u.getConsumption(),
                            u.getCoQuayVong(),
                            u.getMaxRegisterSnapshot(),
                            u.getLanDocPhu(),
                            u.getLoaiGhiIndex(),
                            u.getMaCto(),
                            u.getSoLanQuayVong(),
                            u.getTgianBdien()
                    ))
                    .collect(Collectors.toList());
                    
            BillingTaskDto task = new BillingTaskDto(
                    account.getMaKhang(),
                    dtuongQly,
                    month,
                    effectivePeriod,
                    nextVersion,
                    traceId,
                    readings
            );
            task.setTriggeredBy("BATCH");
            task.setPriority(1);

            // Fetch snapshot configuration to copy flags
            BillingConfigSnapshot config = cmisIngestionListener.getSnapshotConfig(account.getMaKhang(), month, effectivePeriod);
            String finalChangeFlag = "NONE";
            if (config != null && config.getChangeFlags() != null) {
                finalChangeFlag = config.getChangeFlags();
            }
            boolean hasReadingChange = validatedUsages.stream().anyMatch(u -> "CORRECTION".equals(u.getLoaiGhiIndex()) || (u.getLanDocPhu() != null && u.getLanDocPhu() > 1));
            if (hasReadingChange) {
                if ("NONE".equals(finalChangeFlag)) {
                    finalChangeFlag = "READING_CHANGE";
                } else {
                    finalChangeFlag = "MULTI_CHANGE";
                }
            }
            task.setChangeFlags(finalChangeFlag);
            task.setLoaiKhang(config != null ? config.getCustomerType() : "SINH_HOAT");
            task.setHasRelation(config != null && config.isHasRelation());
            return task;
        };
    }

    /**
     * Writes processed task chunks directly into Apache Kafka.
     * Kafka Partition Key is set to AccountId to preserve routing.
     */
    @Bean
    public KafkaItemWriter<String, BillingTaskDto> kafkaItemWriter() {
        kafkaTemplate.setDefaultTopic("billing-execution-topic");
        return new KafkaItemWriterBuilder<String, BillingTaskDto>()
                .kafkaTemplate(kafkaTemplate)
                .itemKeyMapper(BillingTaskDto::getMaKhang)
                .build();
    }

    /**
     * Defines Step with Chunk size of 1000 accounts.
     */
    @Bean
    public Step billingStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                            JpaPagingItemReader<Account> accountReader, ItemProcessor<Account, BillingTaskDto> accountProcessor,
                            KafkaItemWriter<String, BillingTaskDto> kafkaItemWriter) {
        return new StepBuilder("billingStep", jobRepository)
                .<Account, BillingTaskDto>chunk(1000, transactionManager)
                .reader(accountReader)
                .processor(accountProcessor)
                .writer(kafkaItemWriter)
                .build();
    }

    @Bean
    public BillingJobListener billingJobListener() {
        return new BillingJobListener();
    }

    /**
     * Defines Job containing the calculation steps.
     */
    @Bean
    public Job billingJob(JobRepository jobRepository, Step billingStep) {
        return new JobBuilder("billingJob", jobRepository)
                .start(billingStep)
                .listener(billingJobListener())
                .build();
    }
}
