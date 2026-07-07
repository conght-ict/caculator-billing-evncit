package com.evn.billing.snapshot.controller;

import com.evn.billing.snapshot.service.SnapshotGeneratorService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/snapshots")
public class SnapshotController {

    @Autowired
    private SnapshotGeneratorService snapshotGeneratorService;

    @PostMapping("/generate")
    public ResponseEntity<String> generateSnapshots(
            @RequestParam String bookId,
            @RequestParam String month,
            @RequestParam(defaultValue = "1") Integer period) {
        snapshotGeneratorService.generateSnapshotsForBook(bookId, month, period);
        return ResponseEntity.ok("Billing configuration snapshots generated and cache synchronized.");
    }
}
