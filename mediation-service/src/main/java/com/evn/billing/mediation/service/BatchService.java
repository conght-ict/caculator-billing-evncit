package com.evn.billing.mediation.service;

import com.evn.billing.common.domain.DtuongQlySchedule;
import com.evn.billing.mediation.repository.DtuongQlyScheduleRepository;
import com.evn.billing.mediation.repository.MeterUsageRepository;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Service
public class BatchService {

    @Autowired
    private MeterUsageRepository meterUsageRepository;

    @Autowired
    private DtuongQlyScheduleRepository dtuongQlyScheduleRepository;

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job billingJob;

    public boolean validateBookReadiness(String dtuongQly, String month, Integer period) {
        long pendingCount = meterUsageRepository.countPendingReadingsForBook(dtuongQly, month, period);
        return pendingCount == 0;
    }

    public boolean isBookAlreadyCompleted(String dtuongQly, String month, Integer period) {
        Optional<DtuongQlySchedule> runOpt =
                dtuongQlyScheduleRepository.findByDtuongQlyAndThangCkAndKyChot(dtuongQly, month, period);
        return runOpt.isPresent() && "COMPLETED".equals(runOpt.get().getTrangThaiChay());
    }

    public String validateBatch(String dtuongQly, String month, Integer period) {
        if (isBookAlreadyCompleted(dtuongQly, month, period)) {
            return "BOOK_ALREADY_CALCULATED_NEEDS_CANCEL";
        }
        boolean isReady = validateBookReadiness(dtuongQly, month, period);
        return isReady ? "READY_FOR_BILLING" : "READY_CHECK_FAILED_PENDING_MANUAL_READINGS";
    }

    public JobExecution launchBillingJob(String dtuongQly, String month, Integer period, Long version) throws Exception {
        Map<String, JobParameter<?>> params = new HashMap<>();
        params.put("dtuongQly", new JobParameter<>(dtuongQly, String.class));
        params.put("month", new JobParameter<>(month, String.class));
        params.put("period", new JobParameter<>(period.longValue(), Long.class));
        params.put("calculationVersion", new JobParameter<>(version, Long.class));
        params.put("time", new JobParameter<>(System.currentTimeMillis(), Long.class));
        JobParameters jobParameters = new JobParameters(params);
        return jobLauncher.run(billingJob, jobParameters);
    }
}
