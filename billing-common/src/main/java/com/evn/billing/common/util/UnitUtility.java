package com.evn.billing.common.util;

public class UnitUtility {

    public static String getMadviCapTctFromMaDviQly(String maDviQly) {
        if (maDviQly == null || maDviQly.length() < 4) {
            return null;
        }

        String prefix = maDviQly.substring(0, 2);
        String prefixDLuc = maDviQly.substring(0, 4);

        // ---------------- NPC ----------------
        if (prefix.equals("PA") || prefix.equals("PH") || prefix.equals("PM") || prefix.equals("PN")) {
            return "PA";
        }

        // ---------------- SPC ----------------
        if ((prefix.equals("PB") && !prefixDLuc.equals("PB04") && !prefixDLuc.equals("PB15")
                && !prefixDLuc.equals("PB18"))
                || prefix.equals("PK")
                || prefixDLuc.equals("PC13")) {
            return "PB";
        }

        // ---------------- CPC ----------------
        if ((prefix.equals("PC") && !prefixDLuc.equals("PC13"))
                || prefix.equals("PQ")
                || prefix.equals("PP")
                || prefixDLuc.equals("PB18")) {
            return "PC";
        }

        // ---------------- HÀ NỘI ----------------
        if (prefix.equals("PD") || prefixDLuc.startsWith("HN")) {
            return "HN";
        }

        // ---------------- HCM ----------------
        if (prefix.equals("PE")
                || prefixDLuc.equals("PB04")
                || prefixDLuc.equals("PB15")) {
            return "PE";
        }

        return null;
    }
}
