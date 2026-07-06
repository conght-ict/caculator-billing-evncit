package com.evn.billing.snapshot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication
@EntityScan("com.evn.billing.common.domain")
@EnableJpaRepositories("com.evn.billing.snapshot.repository")
public class SnapshotGeneratorApplication {
    public static void main(String[] args) {
        SpringApplication.run(SnapshotGeneratorApplication.class, args);
    }
}
