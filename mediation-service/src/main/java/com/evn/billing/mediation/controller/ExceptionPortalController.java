package com.evn.billing.mediation.controller;

import com.evn.billing.common.dto.ExceptionQueryRequest;
import com.evn.billing.common.dto.BatchValidateRequest;
import com.evn.billing.mediation.service.ExceptionPortalService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1")
public class ExceptionPortalController {

    @Autowired
    private ExceptionPortalService exceptionPortalService;

    @PostMapping("/exceptions")
    public ResponseEntity<List<Map<String, Object>>> getPendingExceptions(@RequestBody ExceptionQueryRequest request) {
        List<Map<String, Object>> exceptions = exceptionPortalService.getPendingExceptions(
                request.getDtuongQly(),
                request.getThangChuKy(),
                request.getKyChot() != null ? request.getKyChot() : 1
        );
        return ResponseEntity.ok(exceptions);
    }

    @PostMapping("/exceptions/resolve")
    public ResponseEntity<Void> resolveException(@RequestBody ResolveRequest request) {
        exceptionPortalService.resolveException(
                request.getIdChiSo(),
                request.getThangChuKy(),
                request.getChiSoCuoiDieuChinh(),
                request.getGhiChuNguoiXuLy()
        );
        return ResponseEntity.ok().build();
    }


    @lombok.Data
    public static class ResolveRequest {
        private Long idChiSo;
        private String thangChuKy;
        private BigDecimal chiSoCuoiDieuChinh;
        private String ghiChuNguoiXuLy;
    }
}
