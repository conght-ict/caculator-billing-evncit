package com.evn.billing.worker;

import com.evn.billing.common.dto.MeterReadingDto;
import com.evn.billing.common.util.ArrowSerializationHelper;
import org.junit.jupiter.api.Test;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

public class InfrastructureCoreTest {

    @Test
    public void testArrowSerializationRoundTrip() {
        // 1. Create mock readings
        List<MeterReadingDto> original = new ArrayList<>();
        
        MeterReadingDto r1 = new MeterReadingDto();
        r1.setMaDdo("METER-01");
        r1.setTuNgay(LocalDateTime.of(2026, 6, 1, 0, 0, 0));
        r1.setDenNgay(LocalDateTime.of(2026, 6, 30, 23, 59, 59));
        r1.setChiSoDau(new BigDecimal("1000.0000"));
        r1.setChiSoCuoi(new BigDecimal("1250.0000"));
        r1.setSanLuong(new BigDecimal("250.0000"));
        original.add(r1);

        MeterReadingDto r2 = new MeterReadingDto();
        r2.setMaDdo("METER-02");
        r2.setTuNgay(LocalDateTime.of(2026, 6, 1, 0, 0, 0));
        r2.setDenNgay(LocalDateTime.of(2026, 6, 30, 23, 59, 59));
        r2.setChiSoDau(new BigDecimal("5000.0000"));
        r2.setChiSoCuoi(new BigDecimal("15000.0000"));
        r2.setSanLuong(new BigDecimal("10000.0000"));
        original.add(r2);

        // 2. Serialize to Apache Arrow columnar binary format
        byte[] arrowBytes = ArrowSerializationHelper.serializeReadings(original);
        assertNotNull(arrowBytes);
        assertTrue(arrowBytes.length > 0);

        // 3. Deserialize back
        List<MeterReadingDto> deserialized = ArrowSerializationHelper.deserializeReadings(arrowBytes);
        assertNotNull(deserialized);
        assertEquals(original.size(), deserialized.size());

        // 4. Verify contents
        for (int i = 0; i < original.size(); i++) {
            MeterReadingDto orig = original.get(i);
            MeterReadingDto dest = deserialized.get(i);
            
            assertEquals(orig.getMaDdo(), dest.getMaDdo());
            assertEquals(orig.getTuNgay(), dest.getTuNgay());
            assertEquals(orig.getDenNgay(), dest.getDenNgay());
            assertEquals(orig.getChiSoDau().setScale(2), dest.getChiSoDau().setScale(2));
            assertEquals(orig.getChiSoCuoi().setScale(2), dest.getChiSoCuoi().setScale(2));
            assertEquals(orig.getSanLuong().setScale(2), dest.getSanLuong().setScale(2));
        }
    }
}
