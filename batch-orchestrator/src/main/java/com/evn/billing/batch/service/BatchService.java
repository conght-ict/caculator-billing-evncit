package com.evn.billing.batch.service;

import com.evn.billing.batch.repository.MeterUsageRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class BatchService {

    @Autowired
    private MeterUsageRepository meterUsageRepository;

    /**
     * Checks if a Book is ready for billing (all accounts in the Book have validated readings for a given period).
     * 
     * @return true if 0 missing or erroneous (PENDING_MANUAL) readings exist, false otherwise.
     */
    public boolean validateBookReadiness(String bookId, String month, Integer period) {
        long pendingCount = meterUsageRepository.countPendingReadingsForBook(bookId, month, period);
        return pendingCount == 0;
    }
}
