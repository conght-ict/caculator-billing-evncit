package com.evn.billing.mediation.controller;

import com.evn.billing.mediation.service.ReadingsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

@RestController
@RequestMapping("/api/v1")
public class MediationController {

    @Autowired
    private ReadingsService readingsService;

    @PostMapping("/readings/legacy")
    public ResponseEntity<String> receiveReadings(@RequestBody List<ReadingDto> readings) {
        readingsService.processAndSaveReadings(readings);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body("Legacy readings processed and saved.");
    }

    public static class ReadingDto {
        private String maKhang;
        private String maDdo;
        private String thangChuKy;
        private Integer kyChot = 1;
        private LocalDateTime tuNgay;
        private LocalDateTime denNgay;
        private BigDecimal chiSoDau;
        private BigDecimal chiSoCuoi;

        public String getMaKhang() { return maKhang; }
        public void setMaKhang(String maKhang) { this.maKhang = maKhang; }
        public String getMaDdo() { return maDdo; }
        public void setMaDdo(String maDdo) { this.maDdo = maDdo; }
        public String getThangChuKy() { return thangChuKy; }
        public void setThangChuKy(String thangChuKy) { this.thangChuKy = thangChuKy; }
        public Integer getKyChot() { return kyChot; }
        public void setKyChot(Integer kyChot) { this.kyChot = kyChot; }
        public LocalDateTime getTuNgay() { return tuNgay; }
        public void setTuNgay(LocalDateTime tuNgay) { this.tuNgay = tuNgay; }
        public LocalDateTime getDenNgay() { return denNgay; }
        public void setDenNgay(LocalDateTime denNgay) { this.denNgay = denNgay; }
        public BigDecimal getChiSoDau() { return chiSoDau; }
        public void setChiSoDau(BigDecimal chiSoDau) { this.chiSoDau = chiSoDau; }
        public BigDecimal getChiSoCuoi() { return chiSoCuoi; }
        public void setChiSoCuoi(BigDecimal chiSoCuoi) { this.chiSoCuoi = chiSoCuoi; }
    }
}
