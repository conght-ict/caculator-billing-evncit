package com.evn.billing.snapshot.service;

import com.evn.billing.common.domain.*;
import com.evn.billing.common.dto.*;
import com.evn.billing.snapshot.repository.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
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
    private TariffDetailRepository tariffDetailRepository;

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    /**
     * Scans active accounts, builds static snapshot profiles based on database relational data
     * (topology tree & tariffs), saves them to the DB snapshot table, and syncs to Redis Cache.
     * 
     * @param bookId The logical book partition ID
     * @param month The billing cycle month (YYYY_MM)
     */
    @Transactional
    public void generateSnapshotsForBook(String bookId, String month) {
        List<Account> accounts = accountRepository.findByBookIdAndStatus(bookId, "ACTIVE");
        if (accounts.isEmpty()) {
            return;
        }

        // Seed sample data for testing if tables are empty
        seedSampleDataIfEmpty(bookId, accounts);

        List<BillingAccountSnapshot> snapshots = new ArrayList<>();

        for (Account account : accounts) {
            BillingConfigSnapshot config = new BillingConfigSnapshot();
            config.setAccountId(account.getAccountId());
            
            // Set shared households normsFactor from Account entity
            int normsFactor = account.getNormsFactor();
            config.setNormsFactor(normsFactor);

            // 1. Query meter points for the account
            List<MeterPoint> meterPoints = meterPointRepository.findByAccountIdAndStatus(account.getAccountId(), "ACTIVE");
            if (meterPoints.isEmpty()) {
                continue;
            }

            // 2. Query relationships for the meter points
            List<String> meterIds = meterPoints.stream().map(MeterPoint::getMeterPointId).toList();
            List<MeterRelation> relations = meterRelationRepository.findRelationsByMeterIds(meterIds);

            // 3. Build Topology tree
            MeterTopology topology = buildTopology(meterPoints, relations);
            config.setMeterTopology(topology);

            // 4. Query and Map Tariffs
            Map<String, TariffRules> tariffs = buildTariffs(meterPoints);
            config.setTariffs(tariffs);

            // 4.1 Set Fast-Path flags
            boolean isFastPath = (meterPoints.size() == 1) && relations.isEmpty();
            config.setFastPathEnabled(isFastPath);
            if (isFastPath) {
                config.setFastPathMeterPointId(meterPoints.get(0).getMeterPointId());
                config.setFastPathTariffCode(meterPoints.get(0).getTariffCode());
            }

            // Determine if main meter is stepping or flat
            String mainTariffCode = isFastPath ? meterPoints.get(0).getTariffCode() : "TARIFF_SHBT_2026";
            TariffRules mainTariff = tariffs.get(mainTariffCode);
            boolean isStepping = mainTariff == null || "STEPPING".equals(mainTariff.getType());

            // 4.2 Populate default billing schema steps
            config.setSchemaSteps(buildDefaultSchemaSteps(isStepping));

            // 5. Create Entity Snapshot
            BillingAccountSnapshot snapshot = new BillingAccountSnapshot();
            String snapshotId = account.getAccountId() + "_" + month + "_v1";
            snapshot.setSnapshotId(snapshotId);
            snapshot.setAccountId(account.getAccountId());
            snapshot.setBookId(bookId);
            snapshot.setBillingCycleMonth(month);
            snapshot.setCalculationVersion(1);
            snapshot.setConfigData(config);
            snapshot.setCreatedAt(LocalDateTime.now());

            snapshots.add(snapshot);

            // 6. Synchronize to Redis Cache (TTL = 24 hours)
            String cacheKey = "snapshot:" + account.getAccountId() + ":" + month;
            try {
                redisTemplate.opsForValue().set(cacheKey, config, 24, TimeUnit.HOURS);
            } catch (Exception e) {
                log.warn("Failed to cache snapshot in Redis: {}", e.getMessage());
            }
        }

        snapshotRepository.saveAll(snapshots);
    }

    /**
     * Builds hierarchical topology tree from flat database rows.
     */
    private MeterTopology buildTopology(List<MeterPoint> meterPoints, List<MeterRelation> relations) {
        Map<String, MeterPointNode> nodeMap = new HashMap<>();
        
        // Initialize all nodes
        for (MeterPoint mp : meterPoints) {
            MeterPointNode node = new MeterPointNode();
            node.setMeterPointId(mp.getMeterPointId());
            node.setTariffCode(mp.getTariffCode());
            node.setChildPoints(new ArrayList<>());
            node.setCalculationType(CalculationType.AGGREGATION);
            nodeMap.put(mp.getMeterPointId(), node);
        }

        Set<String> childNodeIds = new HashSet<>();
        
        // Build parent-child links
        for (MeterRelation rel : relations) {
            MeterPointNode parent = nodeMap.get(rel.getParentId());
            MeterPointNode child = nodeMap.get(rel.getChildId());
            
            if (parent != null && child != null) {
                child.setCalculationType(CalculationType.valueOf(rel.getRelationType()));
                parent.getChildPoints().add(child);
                childNodeIds.add(child.getMeterPointId());
            }
        }

        // Roots are nodes that are NOT children of any other node
        List<MeterPointNode> rootPoints = new ArrayList<>();
        for (MeterPoint mp : meterPoints) {
            if (!childNodeIds.contains(mp.getMeterPointId())) {
                MeterPointNode root = nodeMap.get(mp.getMeterPointId());
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
     * Builds TariffRules configurations for active meter points.
     */
    private Map<String, TariffRules> buildTariffs(List<MeterPoint> meterPoints) {
        List<String> tariffCodes = meterPoints.stream()
                .map(MeterPoint::getTariffCode)
                .filter(Objects::nonNull)
                .distinct()
                .toList();

        if (tariffCodes.isEmpty()) {
            return Collections.emptyMap();
        }

        List<Tariff> tariffsDb = tariffRepository.findAllById(tariffCodes);
        List<TariffDetail> detailsDb = tariffDetailRepository.findByTariffCodeIn(tariffCodes);

        // Group details by tariff code
        Map<String, List<TariffDetail>> detailsMap = new HashMap<>();
        for (TariffDetail detail : detailsDb) {
            detailsMap.computeIfAbsent(detail.getTariffCode(), k -> new ArrayList<>()).add(detail);
        }

        Map<String, TariffRules> tariffs = new HashMap<>();
        for (Tariff t : tariffsDb) {
            TariffRules rules = new TariffRules();
            rules.setTariffCode(t.getTariffCode());
            rules.setType(t.getType());

            List<TariffBlock> blocks = new ArrayList<>();
            List<TariffDetail> details = detailsMap.getOrDefault(t.getTariffCode(), Collections.emptyList());
            
            // Sort by step ascending
            List<TariffDetail> sortedDetails = new ArrayList<>(details);
            sortedDetails.sort(Comparator.comparing(TariffDetail::getStep));

            for (TariffDetail d : sortedDetails) {
                TariffBlock block = new TariffBlock();
                block.setStep(d.getStep());
                block.setMinKwh(d.getMinKwh().doubleValue());
                block.setMaxKwh(d.getMaxKwh() != null ? d.getMaxKwh().doubleValue() : null);
                block.setUnitPrice(d.getUnitPrice().doubleValue());
                blocks.add(block);
            }
            rules.setBlocks(blocks);
            tariffs.put(t.getTariffCode(), rules);
        }

        return tariffs;
    }

    /**
     * Seeds mock tariff and meter relations when database tables are blank.
     */
    private void seedSampleDataIfEmpty(String bookId, List<Account> accounts) {
        if (tariffRepository.count() == 0) {
            Tariff t1 = new Tariff();
            t1.setTariffCode("TARIFF_SHBT_2026");
            t1.setName("Sinh hoạt bậc thang");
            t1.setType("STEPPING");
            tariffRepository.save(t1);

            List<TariffDetail> details = new ArrayList<>();
            details.add(createDetail("TARIFF_SHBT_2026", 1, 0.0, 50.0, 1806.0));
            details.add(createDetail("TARIFF_SHBT_2026", 2, 50.0, 100.0, 1866.0));
            details.add(createDetail("TARIFF_SHBT_2026", 3, 100.0, 200.0, 2167.0));
            details.add(createDetail("TARIFF_SHBT_2026", 4, 200.0, 300.0, 2729.0));
            details.add(createDetail("TARIFF_SHBT_2026", 5, 300.0, 400.0, 3050.0));
            details.add(createDetail("TARIFF_SHBT_2026", 6, 400.0, null, 3157.0));
            tariffDetailRepository.saveAll(details);

            Tariff t2 = new Tariff();
            t2.setTariffCode("TARIFF_KDOANH_2026");
            t2.setName("Kinh doanh đồng giá");
            t2.setType("FLAT");
            tariffRepository.save(t2);

            TariffDetail d2 = createDetail("TARIFF_KDOANH_2026", 1, 0.0, null, 2500.0);
            tariffDetailRepository.save(d2);
        }

        for (Account account : accounts) {
            List<MeterPoint> points = meterPointRepository.findByAccountIdAndStatus(account.getAccountId(), "ACTIVE");
            if (points.isEmpty()) {
                MeterPoint mp1 = new MeterPoint();
                mp1.setMeterPointId("METER-TONG-" + account.getAccountId());
                mp1.setAccountId(account.getAccountId());
                mp1.setTariffCode("TARIFF_SHBT_2026");
                mp1.setStatus("ACTIVE");
                meterPointRepository.save(mp1);

                MeterPoint mp2 = new MeterPoint();
                mp2.setMeterPointId("METER-PHU-" + account.getAccountId());
                mp2.setAccountId(account.getAccountId());
                mp2.setTariffCode("TARIFF_KDOANH_2026");
                mp2.setStatus("ACTIVE");
                meterPointRepository.save(mp2);

                MeterRelation rel = new MeterRelation();
                rel.setParentId(mp1.getMeterPointId());
                rel.setChildId(mp2.getMeterPointId());
                rel.setRelationType("NETTING");
                meterRelationRepository.save(rel);
            }
        }
    }

    private TariffDetail createDetail(String code, int step, double min, Double max, double price) {
        TariffDetail d = new TariffDetail();
        d.setTariffCode(code);
        d.setStep(step);
        d.setMinKwh(BigDecimal.valueOf(min));
        d.setMaxKwh(max != null ? BigDecimal.valueOf(max) : null);
        d.setUnitPrice(BigDecimal.valueOf(price));
        return d;
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
        outputs.put("breakdown", "RATING_BREAKDOWN");
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
        taxConfig.put("taxRate", 0.10);
        taxStep.setStepConfig(taxConfig);
        steps.add(taxStep);

        return steps;
    }
}
