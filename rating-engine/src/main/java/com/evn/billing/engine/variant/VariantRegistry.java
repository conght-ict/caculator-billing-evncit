package com.evn.billing.engine.variant;

import java.util.HashMap;
import java.util.Map;

public class VariantRegistry {
    private static final Map<String, BillingVariant> registry = new HashMap<>();

    static {
        registry.put("STEP_RATING", new SteppingRatingVariant());
        registry.put("FLAT_RATING", new FlatRatingVariant());
        registry.put("PERCENT_DISCOUNT", new PercentDiscountVariant());
        registry.put("TAX", new TaxCalculationVariant());
    }

    public static BillingVariant get(String name) {
        BillingVariant variant = registry.get(name);
        if (variant == null) {
            throw new IllegalArgumentException("Unknown billing variant program: " + name);
        }
        return variant;
    }
}
