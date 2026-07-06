package com.evn.billing.batch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication
@EntityScan("com.evn.billing.common.domain")
@EnableJpaRepositories("com.evn.billing.batch.repository")
public class BatchOrchestratorApplication {
    public static void main(String[] args) {
        SpringApplication.run(BatchOrchestratorApplication.class, args);
    }
}
