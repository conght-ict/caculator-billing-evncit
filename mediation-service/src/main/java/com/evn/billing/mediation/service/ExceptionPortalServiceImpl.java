package com.evn.billing.mediation.service;

import com.evn.billing.mediation.listener.CmisIngestionListener;
import com.evn.billing.mediation.repository.AmrIngestionRepository;
import com.evn.billing.mediation.repository.ExceptionPortalRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@Service
public class ExceptionPortalServiceImpl implements ExceptionPortalService {

    @Autowired
    private ExceptionPortalRepository exceptionPortalRepository;

    @Autowired
    private AmrIngestionRepository amrIngestionRepository;

    @Autowired
    @Lazy
    private CmisIngestionListener cmisIngestionListener;

    @Override
    public List<Map<String, Object>> getPendingExceptions(String dtuongQly, String month, int period) {
        return exceptionPortalRepository.findPendingExceptions(dtuongQly, month, period);
    }

    @Override
    @Transactional
    public void resolveException(Long usageId, String month, BigDecimal correctedEndIndex, String operatorNote) {
        // 1. Resolve exception in DB (mark OLD as REPLACED, INSERT NEW as VALIDATED)
        exceptionPortalRepository.resolveException(usageId, month, correctedEndIndex, operatorNote);

        // 2. Query maKhang and period to trigger recalculation pipeline via Repository
        Map<String, Object> details = exceptionPortalRepository.findAccountAndPeriodByUsageId(usageId, month);
        String maKhang = (String) details.get("ma_khang");
        int period = ((Number) details.get("ky_chot")).intValue();

        // 3. Trigger validation check and automatic billing calculation asynchronously
        cmisIngestionListener.checkAndTriggerBilling(maKhang, month, period, null, System.currentTimeMillis());
    }

    @Override
    public boolean isBookReadyForBilling(String dtuongQly, String month, int period) {
        int pendingCount = exceptionPortalRepository.countPendingExceptions(dtuongQly, month, period);
        return pendingCount == 0;
    }
}
