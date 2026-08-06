package com.evn.billing.engine;

import com.evn.billing.common.dto.BillingConfigSnapshot;
import com.evn.billing.common.exception.MalformSnapshotException;

public class SnapshotValidator {

    /**
     * Validates that the static snapshot data is complete and well-formed.
     * Checks 6 required fields: accountId, dtuongQly, normsFactor, effectiveSyncDate, meterTopology, tariffs.
     * 
     * @param snapshot The snapshot config to validate
     * @throws MalformSnapshotException if validation fails
     */
    public void validate(BillingConfigSnapshot snapshot) {
        if (snapshot == null) {
            throw new MalformSnapshotException("Snapshot config data is null");
        }
        if (snapshot.getMaKhang() == null || snapshot.getMaKhang().trim().isEmpty()) {
            throw new MalformSnapshotException("Snapshot is missing required field: maKhang");
        }
        if (snapshot.getDtuongQly() == null || snapshot.getDtuongQly().trim().isEmpty()) {
            throw new MalformSnapshotException("Snapshot is missing required field: dtuongQly");
        }
        if (snapshot.getSoHo() <= 0) {
            throw new MalformSnapshotException("Snapshot is missing or invalid required field: soHo (must be >= 1)");
        }
        if (snapshot.getNgayHieuLuc() == null) {
            throw new MalformSnapshotException("Snapshot is missing required field: ngayHieuLuc");
        }
        if (snapshot.getMeterTopology() == null || snapshot.getMeterTopology().getRootPoints() == null || snapshot.getMeterTopology().getRootPoints().isEmpty()) {
            throw new MalformSnapshotException("Snapshot is missing required field: meterTopology");
        }
        if (snapshot.getBieuGia() == null || snapshot.getBieuGia().isEmpty()) {
            throw new MalformSnapshotException("Snapshot is missing required field: bieuGia");
        }
    }
}
