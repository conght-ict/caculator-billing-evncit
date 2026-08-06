package com.evn.billing.mediation.repository;

import java.util.Map;
import java.util.Optional;

public interface ReadingResolutionRepository {
    String findBookByAccountId(String accountId);
    Optional<Map<String, Object>> findLatestActiveScheduleByBook(String dtuongQly);
    Long findSuspectOrPendingUsageId(String accountId, String month, int period);
    Long findAnyUsageId(String accountId, String month, int period);
}
