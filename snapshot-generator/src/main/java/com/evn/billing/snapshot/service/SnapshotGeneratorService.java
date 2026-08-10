package com.evn.billing.snapshot.service;

import com.evn.billing.common.domain.*;
import com.evn.billing.common.dto.*;
import com.evn.billing.snapshot.repository.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Service
public class SnapshotGeneratorService {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(SnapshotGeneratorService.class);

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private BillingAccountSnapshotRepository snapshotRepository;

    @Autowired
    private MeterPointRepository meterPointRepository;

    @Autowired
    private MeterRelationRepository meterRelationRepository;

    @Autowired
    private TariffRepository tariffRepository;

    @Autowired
    private DtuongQlyScheduleRepository dtuongQlyScheduleRepository;

    @Autowired
    private DiemDoScheduleRepository diemDoScheduleRepository;

    @Autowired
    private PendingSnapshotChangeRepository pendingSnapshotChangeRepository;

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @Value("${billing.snapshot.default-tariff:TARIFF_SHBT_2023}")
    private String defaultTariffCode;

    @Value("#{${billing.snapshot.grace-periods:{'R-01':3,'R-02':1,'R-03':3,'R-06':7,'R-08':5,'R-09':5,'R-11':7}}}")
    private Map<String, Integer> gracePeriods;

    /**
     * Scans active accounts, builds static snapshot profiles based on database relational data
     * (topology tree & tariffs), saves them to the DB snapshot table, and syncs to Redis Cache.
     * 
     * @param dtuongQly The logical book partition ID
     * @param month The billing cycle month (YYYY_MM)
     */
    @Transactional
    public void generateSnapshotsForBook(String dtuongQly, String month, Integer period) {
        List<Account> accounts = accountRepository.findByDtuongQlyAndStatus(dtuongQly, "ACTIVE");
        if (accounts.isEmpty()) {
            return;
        }

        // Fetch target dates from book schedule (lich_ghi_dqly)
        LocalDate periodFromDate = LocalDate.now().withDayOfMonth(1);
        LocalDate periodToDate = LocalDate.now().withDayOfMonth(LocalDate.now().lengthOfMonth());
        try {
            Optional<DtuongQlySchedule> scheduleOpt = dtuongQlyScheduleRepository.findByDtuongQlyAndThangCkAndKyChot(dtuongQly, month, period);
            if (scheduleOpt.isPresent()) {
                DtuongQlySchedule schedule = scheduleOpt.get();
                if (schedule.getTuNgay() != null) {
                    periodFromDate = schedule.getTuNgay();
                }
                if (schedule.getDenNgay() != null) {
                    periodToDate = schedule.getDenNgay();
                }
            }
        } catch (Exception e) {
            log.warn("No book schedule found for book: {}, month: {}, period: {}. Using default month bounds: {}", dtuongQly, month, period, e.getMessage());
            // Fallback parsing from YYYY_MM
            try {
                String[] parts = month.split("_");
                int year = Integer.parseInt(parts[0]);
                int monthVal = Integer.parseInt(parts[1]);
                if (period == 1) {
                    periodFromDate = LocalDate.of(year, monthVal, 1);
                    periodToDate = LocalDate.of(year, monthVal, 10);
                } else if (period == 2) {
                    periodFromDate = LocalDate.of(year, monthVal, 11);
                    periodToDate = LocalDate.of(year, monthVal, 20);
                } else {
                    periodFromDate = LocalDate.of(year, monthVal, 21);
                    periodToDate = LocalDate.of(year, monthVal, java.time.YearMonth.of(year, monthVal).lengthOfMonth());
                }
            } catch (Exception ex) {
                // Keep default
            }
        }

        List<BillingAccountSnapshot> snapshots = new ArrayList<>();
        Map<String, BillingConfigSnapshot> cacheUpdates = new HashMap<>();

        for (Account account : accounts) {
            // Check LOCKED snapshot state
            if (checkLockedAndLogPending(account.getMaKhang(), month, period, "R-08", "lich_ghi_dqly", "tu_ngay")) {
                continue;
            }

            BillingConfigSnapshot config = new BillingConfigSnapshot();
            config.setMaKhang(account.getMaKhang());
            config.setDtuongQly(dtuongQly);
            config.setTenKhang(account.getTenKhang());
            config.setMaSoThue(account.getMaSoThue());
            config.setDiaChi(account.getDiaChi());
            config.setNgayHieuLuc(LocalDate.now());
            config.setTuNgay(periodFromDate);
            config.setDenNgay(periodToDate);

            // 1. Query meter points for the account
            List<MeterPoint> meterPoints = meterPointRepository.findByMaKhangAndTrangThai(account.getMaKhang(), "ACTIVE");
            if (meterPoints.isEmpty()) {
                continue;
            }
            config.setLoaiKhang(meterPoints.get(0).getLoaiKhang());

            // 2. Query relationships for the meter points
            List<String> meterIds = meterPoints.stream().map(MeterPoint::getMaDdo).toList();
            List<MeterRelation> relations = meterRelationRepository.findRelationsByMeterIds(meterIds);

            // 2.1 Extract pricing rules from MeterPoints (JSONB)
            Map<String, List<PriceApplicationRule>> priceRulesByMeter = new HashMap<>();
            int maxHouseholds = 1;
            int shMeterCount = 0;
            int nshMeterCount = 0;
            int totalMetersWithConfig = 0;

            for (MeterPoint mp : meterPoints) {
                if (mp.getDanhSachApGia() != null && !mp.getDanhSachApGia().isEmpty()) {
                    boolean allSH = true;
                    boolean allNSH = true;
                    for (TariffConfig tc : mp.getDanhSachApGia()) {
                        PriceApplicationRule rule = new PriceApplicationRule();
                        rule.setBbanId(mp.getMaDdo() + "_" + tc.getSoThuTu());
                        rule.setMaDdo(mp.getMaDdo());
                        rule.setSoThuTu(tc.getSoThuTu());
                        rule.setDinhMuc(tc.getDinhMuc());
                        rule.setLoaiDmuc(tc.getLoaiDmuc());
                        rule.setTgianBdien(tc.getTgianBdien());
                        rule.setMaNgia(resolveTariffCode(tc.getMaNgia(), tc.getMaNhomnn(), tc.getMaCapda()));
                        rule.setSoHo(tc.getSoHo());
                        rule.setMaCapda(tc.getMaCapda());
                        rule.setMaNhomnn(tc.getMaNhomnn());

                        priceRulesByMeter.computeIfAbsent(mp.getMaDdo(), k -> new ArrayList<>()).add(rule);
                        if (tc.getSoHo() > maxHouseholds) {
                            maxHouseholds = tc.getSoHo();
                        }

                        String maNhomnn = tc.getMaNhomnn();
                        if (maNhomnn != null && maNhomnn.startsWith("SH")) {
                            allNSH = false;
                        } else {
                            allSH = false;
                        }
                    }
                    totalMetersWithConfig++;
                    if (allSH) {
                        shMeterCount++;
                    } else if (allNSH) {
                        nshMeterCount++;
                    }
                }
            }

            String customerType = "MIXED";
            if (totalMetersWithConfig > 0) {
                if (shMeterCount == totalMetersWithConfig) {
                    customerType = "SINH_HOAT";
                } else if (nshMeterCount == totalMetersWithConfig) {
                    customerType = "NGOAI_SINH_HOAT";
                }
            }
            config.setCustomerType(customerType);
            config.setNormsFactor(maxHouseholds);

            // Sort rules for each meter by soThuTu (so_thu_tu)
            for (List<PriceApplicationRule> rules : priceRulesByMeter.values()) {
                rules.sort(Comparator.comparingInt(PriceApplicationRule::getSoThuTu));
            }

            // 3. Build Topology tree containing frozen price rules
            MeterTopology topology = buildTopology(meterPoints, relations, priceRulesByMeter, month, period);
            config.setMeterTopology(topology);

            // 4. Query and Map Tariffs referenced in the pricing rules (using JSONB blocks)
            Map<String, TariffRules> tariffs = buildTariffs(priceRulesByMeter);
            config.setBieuGia(tariffs);

            // 4.1 Set Fast-Path flags
            boolean isFastPath = (meterPoints.size() == 1) && relations.isEmpty();
            config.setFastPathEnabled(isFastPath);
            config.setChangeFlags("NONE");
            if (isFastPath) {
                String singleMeterId = meterPoints.get(0).getMaDdo();
                config.setFastPathMaDdo(singleMeterId);
                List<PriceApplicationRule> singleRules = priceRulesByMeter.getOrDefault(singleMeterId, Collections.emptyList());
                if (singleRules.isEmpty()) {
                    throw new IllegalStateException("No pricing rules found for single meter point: " + singleMeterId);
                }
                String fastTariff = singleRules.get(0).getMaNgia();
                config.setFastPathMaNgia(fastTariff);
            }

            // Determine if main meter is stepping or flat
            String mainTariffCode = null;
            if (isFastPath) {
                List<PriceApplicationRule> singleRules = priceRulesByMeter.getOrDefault(meterPoints.get(0).getMaDdo(), Collections.emptyList());
                if (!singleRules.isEmpty()) {
                    mainTariffCode = singleRules.get(0).getMaNgia();
                }
            } else {
                // For complex multi-meter or mixed paths, resolve main tariff from root point
                if (!topology.getRootPoints().isEmpty()) {
                    mainTariffCode = topology.getRootPoints().get(0).getMaNgia();
                }
            }
            if (mainTariffCode == null) {
                throw new IllegalStateException("No main tariff code found for account: " + account.getMaKhang());
            }
            TariffRules mainTariff = tariffs.get(mainTariffCode);
            if (mainTariff == null) {
                throw new IllegalStateException("Missing tariff configuration details for code: " + mainTariffCode);
            }
            boolean isStepping = mainTariff.isBacThang();

            // 4.2 Populate default billing schema steps
            config.setSchemaSteps(buildDefaultSchemaSteps(isStepping));
            config.setMaDviqly(account.getMaDviqly());

            // 5. Create Entity Snapshot
            BillingAccountSnapshot snapshot = new BillingAccountSnapshot();
            String snapshotId = account.getMaKhang() + "_" + month + "_p" + period + "_v1";
            snapshot.setIdSnapshot(snapshotId);
            snapshot.setMaKhang(account.getMaKhang());
            snapshot.setDtuongQly(dtuongQly);
            snapshot.setThangChuKy(month);
            snapshot.setKyChot(period);
            snapshot.setPhienBanTinh(1);
            snapshot.setNgayDongBoHieuLuc(LocalDate.now());
            snapshot.setMaDviqly(account.getMaDviqly());
            snapshot.setDuLieuCauHinh(config);
            snapshot.setCreatedAt(LocalDateTime.now());

            snapshots.add(snapshot);

            // Stage Redis update and execute only after DB commit.
            String cacheKey = "snapshot:" + account.getMaKhang() + ":" + month + ":" + period;
            cacheUpdates.put(cacheKey, config);
        }

        snapshotRepository.saveAll(snapshots);

        if (!cacheUpdates.isEmpty()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    for (Map.Entry<String, BillingConfigSnapshot> entry : cacheUpdates.entrySet()) {
                        try {
                            redisTemplate.opsForValue().set(entry.getKey(), entry.getValue(), 24, TimeUnit.HOURS);
                        } catch (Exception e) {
                            log.warn("Failed to cache snapshot in Redis: {}", e.getMessage());
                        }
                    }
                }
            });
        }
    }

    /**
     * Builds hierarchical topology tree from flat database rows and binds price rules.
     */
    private MeterTopology buildTopology(List<MeterPoint> meterPoints, List<MeterRelation> relations, Map<String, List<PriceApplicationRule>> priceRulesByMeter, String month, Integer period) {
        Map<String, MeterPointNode> nodeMap = new HashMap<>();
        
        // Initialize all nodes
        for (MeterPoint mp : meterPoints) {
            MeterPointNode node = new MeterPointNode();
            node.setMaDdo(mp.getMaDdo());
            String serial = "";
            if (mp.getMeterDetailsList() != null && !mp.getMeterDetailsList().isEmpty()) {
                serial = mp.getMeterDetailsList().stream()
                    .filter(m -> "ACTIVE".equals(m.getTrangThai()))
                    .map(MeterDetails::getSoSeri)
                    .findFirst()
                    .orElse(mp.getMeterDetailsList().get(0).getSoSeri());
            }
            node.setMeterSerial(serial);
            
            List<PriceApplicationRule> rules = priceRulesByMeter.getOrDefault(mp.getMaDdo(), Collections.emptyList());
            node.setPriceRules(rules);
            if (!rules.isEmpty()) {
                node.setMaNgia(rules.get(0).getMaNgia());
            } else {
                node.setMaNgia(null);
                log.warn("[TOPOLOGY] MeterPoint {} has no pricing rules - will be aggregation-only in rating.", mp.getMaDdo());
            }

            node.setChildPoints(new ArrayList<>());
            node.setCalculationType(CalculationType.AGGREGATION);
            node.setActiveMeters(mp.getMeterDetailsList());
            node.setLoaiDdo(mp.getLoaiDdo());
            node.setIsDienMt(mp.getIsDienMt());

            // Prioritize meter point schedule (lich_ghi_ddo)
            try {
                Optional<DiemDoSchedule> ddoScheduleOpt = diemDoScheduleRepository.findByMaDdoAndThangCkAndKyChot(mp.getMaDdo(), month, period);
                if (ddoScheduleOpt.isPresent()) {
                    DiemDoSchedule schedule = ddoScheduleOpt.get();
                    if (schedule.getTuNgay() != null) {
                        node.setTuNgay(schedule.getTuNgay());
                    }
                    if (schedule.getDenNgay() != null) {
                        node.setDenNgay(schedule.getDenNgay());
                    }
                }
            } catch (Exception e) {
                node.setTuNgay(null);
                node.setDenNgay(null);
            }

            nodeMap.put(mp.getMaDdo(), node);
        }

        Set<String> childNodeIds = new HashSet<>();
        
        // Build parent-child links
        for (MeterRelation rel : relations) {
            MeterPointNode parent = nodeMap.get(rel.getMaDdoCha());
            MeterPointNode child = nodeMap.get(rel.getMaDdoCon());
            
            if (parent != null && child != null) {
                child.setCalculationType(CalculationType.valueOf(rel.getLoaiQuanHe()));
                parent.getChildPoints().add(child);
                childNodeIds.add(child.getMaDdo());
            }
        }

        // Roots are nodes that are NOT children of any other node
        List<MeterPointNode> rootPoints = new ArrayList<>();
        for (MeterPoint mp : meterPoints) {
            if (!childNodeIds.contains(mp.getMaDdo())) {
                MeterPointNode root = nodeMap.get(mp.getMaDdo());
                if (root != null) {
                    rootPoints.add(root);
                }
            }
        }

        MeterTopology topology = new MeterTopology();
        topology.setRootPoints(rootPoints);
        return topology;
    }

    /**
     * Builds TariffRules configurations for active tariffs referenced in the pricing rules.
     */
    private Map<String, TariffRules> buildTariffs(Map<String, List<PriceApplicationRule>> priceRulesByMeter) {
        List<String> tariffCodes = priceRulesByMeter.values().stream()
                .flatMap(Collection::stream)
                .map(PriceApplicationRule::getMaNgia)
                .filter(Objects::nonNull)
                .distinct()
                .toList();

        if (tariffCodes.isEmpty()) {
            throw new IllegalStateException("No tariff code mapped from price application rules.");
        }

        List<Tariff> tariffsDb = tariffRepository.findAllById(tariffCodes);
        LocalDate now = LocalDate.now();
        // Lọc biểu giá hoạt động và không hết hạn
        tariffsDb = tariffsDb.stream()
                .filter(t -> "ACTIVE".equals(t.getTrangThai()))
                .filter(t -> t.getNgayHieuLuc() == null || !t.getNgayHieuLuc().isAfter(now))
                .filter(t -> t.getNgayHetHan() == null || !t.getNgayHetHan().isBefore(now))
                .toList();

        Set<String> foundTariffCodes = tariffsDb.stream().map(Tariff::getMaNgia).collect(java.util.stream.Collectors.toSet());
        List<String> missingTariffCodes = tariffCodes.stream().filter(code -> !foundTariffCodes.contains(code)).toList();
        if (!missingTariffCodes.isEmpty()) {
            throw new IllegalStateException("Missing tariff master data or expired for codes: " + missingTariffCodes);
        }

        Map<String, TariffRules> tariffs = new HashMap<>();
        for (Tariff t : tariffsDb) {
            TariffRules rules = new TariffRules();
            rules.setLoaiBieuGia(t.getLoaiBieuGia());
            rules.setBacThang(t.isBacThang());
            rules.setDonGiaPhang(t.getDonGiaPhang());
            rules.setBlocks(t.getBlocks() != null ? t.getBlocks() : new ArrayList<>());
            tariffs.put(t.getMaNgia(), rules);
        }

        return tariffs;
    }



    private List<BillingSchemaStep> buildDefaultSchemaSteps(boolean isStepping) {
        List<BillingSchemaStep> steps = new ArrayList<>();

        // Step 10: Rating (Stepping or Flat)
        BillingSchemaStep ratingStep = new BillingSchemaStep();
        ratingStep.setStepNumber(10);
        ratingStep.setVariantName(isStepping ? "STEP_RATING" : "FLAT_RATING");
        
        Map<String, String> inputs = new HashMap<>();
        inputs.put("consumption", "NET_KWH");
        inputs.put("tariffCode", "FAST_TARIFF_CODE");
        ratingStep.setInputOperands(inputs);

        Map<String, String> outputs = new HashMap<>();
        outputs.put("amount", "BASE_AMOUNT");
        outputs.put("breakdown", "BILL_STEP_DETAILS");
        ratingStep.setOutputOperands(outputs);
        
        ratingStep.setStepConfig(new HashMap<>());
        steps.add(ratingStep);

        // Step 20: VAT Tax Calculation
        BillingSchemaStep taxStep = new BillingSchemaStep();
        taxStep.setStepNumber(20);
        taxStep.setVariantName("TAX");

        Map<String, String> taxInputs = new HashMap<>();
        taxInputs.put("amount", "BASE_AMOUNT");
        taxStep.setInputOperands(taxInputs);

        Map<String, String> taxOutputs = new HashMap<>();
        taxOutputs.put("taxAmount", "TAX_AMOUNT");
        taxOutputs.put("totalAmount", "TOTAL_AMOUNT");
        taxStep.setOutputOperands(taxOutputs);

        Map<String, Object> taxConfig = new HashMap<>();
        taxConfig.put("taxRate", 0.08);
        taxStep.setStepConfig(taxConfig);
        steps.add(taxStep);

        return steps;
    }

    @Transactional
    public void generateSnapshotForAccount(String maKhang, String month, Integer period) {
        generateSnapshotForAccount(maKhang, month, period, "R-01", "diem_do", "danh_sach_ap_gia");
    }

    @Transactional
    public void generateSnapshotForAccount(String maKhang, String month, Integer period, String ruleId, String bangNguon, String truongThayDoi) {
        if (checkLockedAndLogPending(maKhang, month, period, ruleId, bangNguon, truongThayDoi)) {
            return;
        }

        Account account = accountRepository.findById(maKhang).orElse(null);
        if (account == null) {
            log.warn("Account not found for snapshot generation: {}", maKhang);
            return;
        }

        List<MeterPoint> meterPoints = meterPointRepository.findByMaKhangAndTrangThai(account.getMaKhang(), "ACTIVE");
        if (meterPoints.isEmpty()) {
            return;
        }
        String dtuongQly = meterPoints.get(0).getDtuongQly();

        LocalDate periodFromDate = LocalDate.now().withDayOfMonth(1);
        LocalDate periodToDate = LocalDate.now().withDayOfMonth(LocalDate.now().lengthOfMonth());
        try {
            Optional<DtuongQlySchedule> scheduleOpt = dtuongQlyScheduleRepository.findByDtuongQlyAndThangCkAndKyChot(dtuongQly, month, period);
            if (scheduleOpt.isPresent()) {
                DtuongQlySchedule schedule = scheduleOpt.get();
                if (schedule.getTuNgay() != null) {
                    periodFromDate = schedule.getTuNgay();
                }
                if (schedule.getDenNgay() != null) {
                    periodToDate = schedule.getDenNgay();
                }
            }
        } catch (Exception e) {
            log.warn("No book schedule found for account: {}, month: {}, period: {}. Using default month bounds: {}", maKhang, month, period, e.getMessage());
            try {
                String[] parts = month.split("_");
                int year = Integer.parseInt(parts[0]);
                int monthVal = Integer.parseInt(parts[1]);
                if (period == 1) {
                    periodFromDate = LocalDate.of(year, monthVal, 1);
                    periodToDate = LocalDate.of(year, monthVal, 10);
                } else if (period == 2) {
                    periodFromDate = LocalDate.of(year, monthVal, 11);
                    periodToDate = LocalDate.of(year, monthVal, 20);
                } else {
                    periodFromDate = LocalDate.of(year, monthVal, 21);
                    periodToDate = LocalDate.of(year, monthVal, java.time.YearMonth.of(year, monthVal).lengthOfMonth());
                }
            } catch (Exception ex) {
                // Keep default
            }
        }

        BillingConfigSnapshot config = new BillingConfigSnapshot();
        config.setMaKhang(account.getMaKhang());
        config.setDtuongQly(dtuongQly);
        config.setTenKhang(account.getTenKhang());
        config.setMaSoThue(account.getMaSoThue());
        config.setDiaChi(account.getDiaChi());
        config.setNgayHieuLuc(LocalDate.now());
        config.setTuNgay(periodFromDate);
        config.setDenNgay(periodToDate);


        config.setLoaiKhang(meterPoints.get(0).getLoaiKhang());

        List<String> meterIds = meterPoints.stream().map(MeterPoint::getMaDdo).toList();
        List<MeterRelation> relations = meterRelationRepository.findRelationsByMeterIds(meterIds);

        Map<String, List<PriceApplicationRule>> priceRulesByMeter = new HashMap<>();
        int maxHouseholds = 1;
        int shMeterCount = 0;
        int nshMeterCount = 0;
        int totalMetersWithConfig = 0;

        for (MeterPoint mp : meterPoints) {
            if (mp.getDanhSachApGia() != null && !mp.getDanhSachApGia().isEmpty()) {
                boolean allSH = true;
                boolean allNSH = true;
                for (TariffConfig tc : mp.getDanhSachApGia()) {
                    PriceApplicationRule rule = new PriceApplicationRule();
                    rule.setBbanId(mp.getMaDdo() + "_" + tc.getSoThuTu());
                    rule.setMaDdo(mp.getMaDdo());
                    rule.setSoThuTu(tc.getSoThuTu());
                    rule.setDinhMuc(tc.getDinhMuc());
                    rule.setLoaiDmuc(tc.getLoaiDmuc());
                    rule.setTgianBdien(tc.getTgianBdien());
                    rule.setMaNgia(resolveTariffCode(tc.getMaNgia(), tc.getMaNhomnn(), tc.getMaCapda()));
                    rule.setSoHo(tc.getSoHo());
                    rule.setMaCapda(tc.getMaCapda());
                    rule.setMaNhomnn(tc.getMaNhomnn());

                    priceRulesByMeter.computeIfAbsent(mp.getMaDdo(), k -> new ArrayList<>()).add(rule);
                    if (tc.getSoHo() > maxHouseholds) {
                        maxHouseholds = tc.getSoHo();
                    }

                    String maNhomnn = tc.getMaNhomnn();
                    if (maNhomnn != null && maNhomnn.startsWith("SH")) {
                        allNSH = false;
                    } else {
                        allSH = false;
                    }
                }
                totalMetersWithConfig++;
                if (allSH) {
                    shMeterCount++;
                } else if (allNSH) {
                    nshMeterCount++;
                }
            }
        }

        String customerType = "MIXED";
        if (totalMetersWithConfig > 0) {
            if (shMeterCount == totalMetersWithConfig) {
                customerType = "SINH_HOAT";
            } else if (nshMeterCount == totalMetersWithConfig) {
                customerType = "NGOAI_SINH_HOAT";
            }
        }
        config.setCustomerType(customerType);
        config.setNormsFactor(maxHouseholds);

        for (List<PriceApplicationRule> rules : priceRulesByMeter.values()) {
            rules.sort(Comparator.comparingInt(PriceApplicationRule::getSoThuTu));
        }

        MeterTopology topology = buildTopology(meterPoints, relations, priceRulesByMeter, month, period);
        config.setMeterTopology(topology);

        Map<String, TariffRules> tariffs = buildTariffs(priceRulesByMeter);
        config.setBieuGia(tariffs);

        boolean isFastPath = (meterPoints.size() == 1) && relations.isEmpty();
        config.setFastPathEnabled(isFastPath);
        if (isFastPath) {
            String singleMeterId = meterPoints.get(0).getMaDdo();
            config.setFastPathMaDdo(singleMeterId);
            List<PriceApplicationRule> singleRules = priceRulesByMeter.getOrDefault(singleMeterId, Collections.emptyList());
            if (singleRules.isEmpty()) {
                throw new IllegalStateException("No pricing rules found for single meter point: " + singleMeterId);
            }
            String fastTariff = singleRules.get(0).getMaNgia();
            config.setFastPathMaNgia(fastTariff);
        }

        String mainTariffCode = null;
        if (isFastPath) {
            List<PriceApplicationRule> singleRules = priceRulesByMeter.getOrDefault(meterPoints.get(0).getMaDdo(), Collections.emptyList());
            if (!singleRules.isEmpty()) {
                mainTariffCode = singleRules.get(0).getMaNgia();
            }
        } else {
            if (!topology.getRootPoints().isEmpty()) {
                mainTariffCode = topology.getRootPoints().get(0).getMaNgia();
            }
        }
        if (mainTariffCode == null) {
            throw new IllegalStateException("No main tariff code found for account: " + maKhang);
        }
        TariffRules mainTariff = tariffs.get(mainTariffCode);
        if (mainTariff == null) {
            throw new IllegalStateException("Missing tariff configuration details for code: " + mainTariffCode);
        }
        boolean isStepping = mainTariff.isBacThang();

        config.setSchemaSteps(buildDefaultSchemaSteps(isStepping));
        config.setMaDviqly(account.getMaDviqly());

        Optional<BillingAccountSnapshot> oldSnapOpt = snapshotRepository.findByMaKhangAndThangChuKyAndKyChot(account.getMaKhang(), month, period);
        String changeFlags = "NONE";
        if (oldSnapOpt.isPresent()) {
            BillingConfigSnapshot oldConfig = oldSnapOpt.get().getDuLieuCauHinh();
            changeFlags = determineChangeFlags(oldConfig, config);
        }
        config.setChangeFlags(changeFlags);

        BillingAccountSnapshot snapshot = new BillingAccountSnapshot();
        String snapshotId = account.getMaKhang() + "_" + month + "_p" + period + "_v1";
        snapshot.setIdSnapshot(snapshotId);
        snapshot.setMaKhang(account.getMaKhang());
        snapshot.setDtuongQly(dtuongQly);
        snapshot.setThangChuKy(month);
        snapshot.setKyChot(period);
        snapshot.setPhienBanTinh(1);
        snapshot.setNgayDongBoHieuLuc(LocalDate.now());
        snapshot.setMaDviqly(account.getMaDviqly());
        snapshot.setDuLieuCauHinh(config);
        snapshot.setCreatedAt(LocalDateTime.now());

        snapshotRepository.save(snapshot);

        String cacheKey = "snapshot:" + account.getMaKhang() + ":" + month + ":" + period;
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                try {
                    redisTemplate.opsForValue().set(cacheKey, config, 24, TimeUnit.HOURS);
                    log.info("Successfully regenerated and cached snapshot for account: {}", maKhang);
                } catch (Exception e) {
                    log.warn("Failed to cache snapshot in Redis for account: {}", maKhang, e);
                }
            }
        });
    }

    private boolean checkLockedAndLogPending(String maKhang, String month, Integer period, String ruleId, String bangNguon, String truongThayDoi) {
        try {
            Optional<BillingAccountSnapshot> snapOpt = snapshotRepository.findByMaKhangAndThangChuKyAndKyChot(maKhang, month, period);
            if (snapOpt.isPresent() && "LOCKED".equalsIgnoreCase(snapOpt.get().getTrangThai())) {
                log.info("[SNAP-LOCKED] Snapshot for account: {}, month: {}, period: {} is LOCKED. Logging pending change.", maKhang, month, period);
                
                PendingSnapshotChange pending = new PendingSnapshotChange();
                pending.setMaKhang(maKhang);
                pending.setThangChuKy(month);
                pending.setKyChot(period);
                pending.setRuleId(ruleId);
                pending.setBangNguon(bangNguon);
                pending.setTruongThayDoi(truongThayDoi);
                pending.setGraceExpiresAt(LocalDateTime.now().plusDays(getGracePeriodDays(ruleId)));
                pending.setTrangThai("PENDING");
                
                pendingSnapshotChangeRepository.save(pending);
                return true; // Is LOCKED
            }
        } catch (Exception e) {
            log.error("Error checking LOCKED snapshot state", e);
        }
        return false;
    }

    private int getGracePeriodDays(String ruleId) {
        if (gracePeriods != null && gracePeriods.containsKey(ruleId)) {
            return gracePeriods.get(ruleId);
        }
        return 7;
    }

    private String determineChangeFlags(BillingConfigSnapshot oldConfig, BillingConfigSnapshot newConfig) {
        if (oldConfig == null) {
            return "NONE";
        }
        boolean priceChanged = false;
        boolean meterChanged = false;

        // 1. Compare Tariffs and House Norms Factor and Customer Type
        if (!Objects.equals(oldConfig.getBieuGia(), newConfig.getBieuGia())
                || oldConfig.getNormsFactor() != newConfig.getNormsFactor()
                || !Objects.equals(oldConfig.getCustomerType(), newConfig.getCustomerType())) {
            priceChanged = true;
        }

        // 2. Compare Topology and relationship structural changes
        if (!Objects.equals(oldConfig.getMeterTopology(), newConfig.getMeterTopology())
                || oldConfig.isHasRelation() != newConfig.isHasRelation()) {
            meterChanged = true;
        }

        if (priceChanged && meterChanged) {
            return "MULTI_CHANGE";
        } else if (priceChanged) {
            return "PRICE_CHANGE";
        } else if (meterChanged) {
            return "METER_CHANGE";
        }
        return oldConfig.getChangeFlags() != null ? oldConfig.getChangeFlags() : "NONE";
    }

    private String resolveTariffCode(String maNgia, String maNhomnn, String maCapda) {
        if (maNgia == null || maNgia.isEmpty() || maNhomnn == null || maNhomnn.isEmpty()) {
            return defaultTariffCode;
        }
        if ("A".equals(maNgia) || "D".equals(maNgia) || maNgia.length() <= 2) {
            String resolvedCapda = "1".equals(maCapda) ? "2" : maCapda;
            if ("A".equals(maNgia)) {
                return "TARIFF_" + maNhomnn + "_CAPDA" + resolvedCapda + "_20250510";
            } else {
                return "TARIFF_" + maNhomnn + "_" + maNgia + "_CAPDA" + resolvedCapda + "_20250510";
            }
        }
        return maNgia;
    }
}
