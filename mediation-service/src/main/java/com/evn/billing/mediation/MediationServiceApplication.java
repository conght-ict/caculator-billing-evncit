package com.evn.billing.mediation;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = "com.evn.billing")
@EntityScan("com.evn.billing.common.domain")
@EnableJpaRepositories("com.evn.billing.mediation.repository")
@EnableScheduling
public class MediationServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(MediationServiceApplication.class, args);
    }
}
