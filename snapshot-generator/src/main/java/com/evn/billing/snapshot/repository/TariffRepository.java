package com.evn.billing.snapshot.repository;

import com.evn.billing.common.domain.Tariff;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TariffRepository extends JpaRepository<Tariff, String> {
}
