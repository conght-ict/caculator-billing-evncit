package com.evn.billing.worker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication
@EntityScan("com.evn.billing.common.domain")
@EnableJpaRepositories("com.evn.billing.worker.repository")
@org.springframework.scheduling.annotation.EnableScheduling
public class BillingWorkerApplication {
    public static void main(String[] args) {
        SpringApplication.run(BillingWorkerApplication.class, args);
    }
}
