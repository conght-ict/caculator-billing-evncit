package com.evn.billing.mediation.validation;

import org.springframework.stereotype.Component;

@Component
public class SolarMeterBcsAdapter {

    public String adaptBcs(String rawBcs, boolean isDienMt) {
        if (!isDienMt || rawBcs == null || rawBcs.isEmpty()) {
            return rawBcs;
        }
        char first = Character.toUpperCase(rawBcs.charAt(0));
        if (first == 'B') return "BT";
        if (first == 'C') return "CD";
        if (first == 'T') return "TD";
        if (first == 'K') return "KT";
        if (first == 'V') return "VC";
        return rawBcs;
    }
}
