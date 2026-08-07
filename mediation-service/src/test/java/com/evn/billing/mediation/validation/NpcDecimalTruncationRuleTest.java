package com.evn.billing.mediation.validation;

import com.evn.billing.mediation.repository.ValidationQueryRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import java.math.BigDecimal;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class NpcDecimalTruncationRuleTest {

    @Mock
    private ValidationQueryRepository validationQueryRepository;

    @InjectMocks
    private NpcDecimalTruncationRule rule;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testBypassNonNpc() {
        // Given a Hanoi customer (PD0100 -> HN)
        String maKhang = "KH_HANOI";
        when(validationQueryRepository.findMaDviqlyByAccount(maKhang))
                .thenReturn("PD0100");

        ValidationResult result = new ValidationResult();

        // When
        rule.check(maKhang, "2026_08", 1, result);

        // Then
        assertTrue(result.isValid());
        // Verify that no select from chi_so_dien_nang was run
        verify(validationQueryRepository, never()).findNonReplacedReadings(anyString(), anyString(), anyInt());
    }

    @Test
    public void testCheckNpcViolation() {
        // Given an NPC customer (PA1100 -> PA)
        String maKhang = "KH_NPC";
        when(validationQueryRepository.findMaDviqlyByAccount(maKhang))
                .thenReturn("PA1100");

        List<Map<String, Object>> mockReadings = new ArrayList<>();
        Map<String, Object> r1 = new HashMap<>();
        r1.put("ma_ddo", "MP-01");
        r1.put("tgian_bdien", "BT");
        r1.put("chi_so_cuoi", new BigDecimal("123.45")); // violating decimal
        r1.put("ma_cto", "CTO-01");
        mockReadings.add(r1);

        when(validationQueryRepository.findNonReplacedReadings(maKhang, "2026_08", 1))
                .thenReturn(mockReadings);

        ValidationResult result = new ValidationResult();

        // When
        rule.check(maKhang, "2026_08", 1, result);

        // Then
        assertFalse(result.isValid());
        assertEquals(1, result.getErrors().size());
        assertTrue(result.getErrors().get(0).contains("ERR_NPC_DECIMAL_VIOLATION"));
    }

    @Test
    public void testNpcNoViolation() {
        // Given an NPC customer (PA1100 -> PA)
        String maKhang = "KH_NPC_OK";
        when(validationQueryRepository.findMaDviqlyByAccount(maKhang))
                .thenReturn("PA1100");

        List<Map<String, Object>> mockReadings = new ArrayList<>();
        Map<String, Object> r1 = new HashMap<>();
        r1.put("ma_ddo", "MP-01");
        r1.put("tgian_bdien", "BT");
        r1.put("chi_so_cuoi", new BigDecimal("123.00")); // no decimal violation
        r1.put("ma_cto", "CTO-01");
        mockReadings.add(r1);

        when(validationQueryRepository.findNonReplacedReadings(maKhang, "2026_08", 1))
                .thenReturn(mockReadings);

        ValidationResult result = new ValidationResult();

        // When
        rule.check(maKhang, "2026_08", 1, result);

        // Then
        assertTrue(result.isValid());
    }

    @Test
    public void testOverloadedCheckBypassNonNpc() {
        com.evn.billing.common.dto.BillingConfigSnapshot config = new com.evn.billing.common.dto.BillingConfigSnapshot();
        config.setMaDviqly("PD0100"); // Hanoi -> HN

        List<com.evn.billing.common.domain.MeterUsage> usages = new ArrayList<>();
        com.evn.billing.common.domain.MeterUsage u1 = new com.evn.billing.common.domain.MeterUsage();
        u1.setMaKhang("KH_HANOI");
        u1.setThangChuKy("2026_08");
        u1.setKyChot(1);
        u1.setTgianBdien("BT");
        u1.setChiSoCuoi(new BigDecimal("123.45")); // would violate if NPC
        u1.setMaCto("CTO-01");
        u1.setTrangThaiXuLy("VALIDATED");
        usages.add(u1);

        ValidationResult result = new ValidationResult();
        rule.check("KH_HANOI", "2026_08", 1, config, usages, result);

        assertTrue(result.isValid());
        verify(validationQueryRepository, never()).findNonReplacedReadings(anyString(), anyString(), anyInt());
    }

    @Test
    public void testOverloadedCheckNpcViolation() {
        com.evn.billing.common.dto.BillingConfigSnapshot config = new com.evn.billing.common.dto.BillingConfigSnapshot();
        config.setMaDviqly("PA1100"); // NPC -> PA

        List<com.evn.billing.common.domain.MeterUsage> usages = new ArrayList<>();
        com.evn.billing.common.domain.MeterUsage u2 = new com.evn.billing.common.domain.MeterUsage();
        u2.setMaKhang("KH_NPC");
        u2.setThangChuKy("2026_08");
        u2.setKyChot(1);
        u2.setTgianBdien("BT");
        u2.setChiSoCuoi(new BigDecimal("123.45")); // violating decimal
        u2.setMaCto("CTO-01");
        u2.setTrangThaiXuLy("VALIDATED");
        usages.add(u2);

        ValidationResult result = new ValidationResult();
        rule.check("KH_NPC", "2026_08", 1, config, usages, result);

        assertFalse(result.isValid());
        assertEquals(1, result.getErrors().size());
        assertTrue(result.getErrors().get(0).contains("ERR_NPC_DECIMAL_VIOLATION"));
        verify(validationQueryRepository, never()).findNonReplacedReadings(anyString(), anyString(), anyInt());
    }
}
