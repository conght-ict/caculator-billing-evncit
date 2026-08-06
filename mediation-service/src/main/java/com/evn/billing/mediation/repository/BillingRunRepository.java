package com.evn.billing.mediation.repository;

public interface BillingRunRepository {
    int countActiveAccountsInBook(String dtuongQly);
    int countSuccessAccounts(String dtuongQly, String month, int period);
    void transitionSuccessToSuccessCmis(String dtuongQly, String month, int period);
    String findMaDviqlyByBook(String dtuongQly);
    void upsertBookRunProcessing(String dtuongQly, String month, int period, int totalAccounts, int alreadyCalculated, String maDviqly);
    void transitionEligibleAccountsToProcessing(String dtuongQly, String month, int period);
    void updateBookRunFinalStatus(String dtuongQly, String month, int period, String scheduleRunStatus);
}
