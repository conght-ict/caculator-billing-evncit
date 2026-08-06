package com.evn.billing.snapshot.controller;

import com.evn.billing.common.dto.GenerateSnapshotRequest;
import com.evn.billing.common.dto.GenerateAccountSnapshotRequest;
import com.evn.billing.snapshot.service.SnapshotGeneratorService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/snapshots")
public class SnapshotController {

    @Autowired
    private SnapshotGeneratorService snapshotGeneratorService;

    @PostMapping("/generate")
    public ResponseEntity<String> generateSnapshots(@RequestBody GenerateSnapshotRequest request) {
        snapshotGeneratorService.generateSnapshotsForBook(
                request.getDtuongQly(),
                request.getThangChuKy(),
                request.getKyChot() != null ? request.getKyChot() : 1
        );
        return ResponseEntity.ok("Billing configuration snapshots generated and cache synchronized.");
    }

    @PostMapping("/generate-for-account")
    public ResponseEntity<String> generateSnapshotForAccount(@RequestBody GenerateAccountSnapshotRequest request) {
        snapshotGeneratorService.generateSnapshotForAccount(
                request.getMaKhang(),
                request.getThangChuKy(),
                request.getKyChot() != null ? request.getKyChot() : 1,
                request.getRuleId() != null ? request.getRuleId() : "R-01",
                request.getBangNguon() != null ? request.getBangNguon() : "diem_do",
                request.getTruongThayDoi() != null ? request.getTruongThayDoi() : "danh_sach_ap_gia"
        );
        return ResponseEntity.ok("Billing configuration snapshot for account " + request.getMaKhang() + " regenerated and cache synchronized.");
    }
}
