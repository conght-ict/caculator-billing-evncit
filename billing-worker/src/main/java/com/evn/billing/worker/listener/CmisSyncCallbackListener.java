package com.evn.billing.worker.listener;

import com.evn.billing.worker.service.CmisInvoiceSyncService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class CmisSyncCallbackListener {

    private static final Logger log = LoggerFactory.getLogger(CmisSyncCallbackListener.class);

    @Autowired
    private CmisInvoiceSyncService cmisInvoiceSyncService;

    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Lắng nghe sự kiện đồng bộ thành công hóa đơn từ CMIS qua Kafka.
     * Cập nhật mapping ID của CMIS (ID hóa đơn và ID hóa đơn chi tiết của từng điểm đo) vào PostgreSQL.
     */
    @KafkaListener(
            topics = "cmis-sync-callback-topic",
            groupId = "cmis-sync-callback-group",
            containerFactory = "operationsKafkaListenerContainerFactory"
    )
    public void listenCmisSyncCallback(String message) {
        log.info("[CMIS-CALLBACK] Received sync callback event: {}", message);
        try {
            Map<String, Object> payload = objectMapper.readValue(message, new TypeReference<Map<String, Object>>() {});
            
            String idHoaDon = (String) payload.get("id_hoa_don");
            String thangChuKy = (String) payload.get("thang_chu_ky");
            Number cmisIdHdonNum = (Number) payload.get("cmis_id_hdon");
            Map<String, Object> rawMapping = (Map<String, Object>) payload.get("ma_ddo_mapping");

            if (idHoaDon == null || idHoaDon.trim().isEmpty() ||
                thangChuKy == null || thangChuKy.trim().isEmpty() ||
                cmisIdHdonNum == null || rawMapping == null) {
                log.error("[CMIS-CALLBACK] Invalid payload. Missing critical fields in message: {}", message);
                return;
            }

            Long cmisIdHdon = cmisIdHdonNum.longValue();
            Map<String, Long> maDdoToCtietId = new HashMap<>();
            for (Map.Entry<String, Object> entry : rawMapping.entrySet()) {
                if (entry.getValue() instanceof Number) {
                    maDdoToCtietId.put(entry.getKey(), ((Number) entry.getValue()).longValue());
                } else if (entry.getValue() instanceof String) {
                    try {
                        maDdoToCtietId.put(entry.getKey(), Long.parseLong((String) entry.getValue()));
                    } catch (NumberFormatException e) {
                        log.warn("[CMIS-CALLBACK] Cannot parse cmis_id_hdonctiet as Long: {}", entry.getValue());
                    }
                }
            }

            cmisInvoiceSyncService.updateCmisMapping(idHoaDon, thangChuKy, cmisIdHdon, maDdoToCtietId);
            log.info("[CMIS-CALLBACK] Successfully processed sync mapping for invoice: {}", idHoaDon);

        } catch (Exception e) {
            log.error("[CMIS-CALLBACK] Error processing sync callback message: {}", e.getMessage(), e);
        }
    }
}
