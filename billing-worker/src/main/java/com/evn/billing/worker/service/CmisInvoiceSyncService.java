package com.evn.billing.worker.service;

import com.evn.billing.common.domain.BillInvoice;
import com.evn.billing.common.domain.BillInvoiceId;
import com.evn.billing.worker.repository.BillInvoiceRepository;
import com.evn.billing.worker.repository.BillingStateRepository;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

@Service
public class CmisInvoiceSyncService {

    private static final Logger log = LoggerFactory.getLogger(CmisInvoiceSyncService.class);

    @Autowired
    private BillInvoiceRepository billInvoiceRepository;

    @Autowired
    private BillingStateRepository billingStateRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Transactional
    public void updateCmisMapping(String idHoaDon, String thangChuKy,
                                  Long cmisIdHdon, Map<String, Long> maDdoToCtietId) throws Exception {
        BillInvoiceId compositeId = new BillInvoiceId(idHoaDon, thangChuKy);
        Optional<BillInvoice> opt = billInvoiceRepository.findById(compositeId);
        if (opt.isEmpty()) {
            throw new NoSuchElementException("Invoice not found: " + idHoaDon + ", month: " + thangChuKy);
        }

        BillInvoice invoice = opt.get();
        String currentJson = invoice.getChiTietDiemDo();
        List<Map<String, Object>> ctietList = new ArrayList<>();
        if (currentJson != null && !currentJson.trim().isEmpty()) {
            ctietList = objectMapper.readValue(currentJson, new TypeReference<List<Map<String, Object>>>() {});
        }

        for (Map<String, Object> item : ctietList) {
            String maDdo = (String) item.get("ma_ddo");
            if (maDdoToCtietId.containsKey(maDdo)) {
                item.put("cmis_id_hdonctiet", maDdoToCtietId.get(maDdo));
            }
        }

        String updatedJson = objectMapper.writeValueAsString(ctietList);
        billingStateRepository.updateCmisIdMapping(idHoaDon, thangChuKy, cmisIdHdon, updatedJson, "SYNCED");
        log.info("[CMIS-SYNC] Mapped cmis_id_hdon={} for invoice={}", cmisIdHdon, idHoaDon);
    }
}
