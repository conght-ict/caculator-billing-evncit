package com.evn.billing.mediation.repository;

import java.util.Map;
import java.util.Optional;

public interface ReadingResolutionRepository {
    String findBookByAccountId(String maKhang);
    Optional<Map<String, Object>> findLatestActiveScheduleByBook(String dtuongQly);
    Long findSuspectOrPendingUsageId(String maKhang, String month, int period);
    Long findAnyUsageId(String maKhang, String month, int period);
}
