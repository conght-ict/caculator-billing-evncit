package com.evn.billing.engine.variant;

import com.evn.billing.common.dto.BillingSchemaStep;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Stateless expression evaluator variant that parses and executes dynamic expressions
 * like multiplication, division, and conditionals configured in the database without external dependencies.
 */
public class ExpressionEvalVariant implements BillingVariant {

    @Override
    public void execute(Map<String, Object> operands, BillingSchemaStep step) throws Exception {
        String expr = (String) step.getStepConfig().get("expression");
        if (expr == null || expr.isEmpty()) {
            throw new NoSuchElementException("Expression config missing for step");
        }

        // Clean whitespaces
        expr = expr.trim().replaceAll("\\s+", "");

        BigDecimal result = BigDecimal.ZERO;

        // Pattern 1: Conditional [Variable]>[Limit]?[Val1]:[Val2]
        if (expr.contains("?") && expr.contains(":")) {
            int qIndex = expr.indexOf('?');
            int cIndex = expr.indexOf(':');
            String condition = expr.substring(0, qIndex);
            String val1Str = expr.substring(qIndex + 1, cIndex);
            String val2Str = expr.substring(cIndex + 1);

            if (condition.contains(">")) {
                String[] parts = condition.split(">");
                BigDecimal left = resolveValue(parts[0], operands);
                BigDecimal right = new BigDecimal(parts[1]);
                
                BigDecimal val1 = resolveValue(val1Str, operands);
                BigDecimal val2 = resolveValue(val2Str, operands);

                if (left.compareTo(right) > 0) {
                    result = val1;
                } else {
                    result = val2;
                }
            } else if (condition.contains("<")) {
                String[] parts = condition.split("<");
                BigDecimal left = resolveValue(parts[0], operands);
                BigDecimal right = new BigDecimal(parts[1]);

                BigDecimal val1 = resolveValue(val1Str, operands);
                BigDecimal val2 = resolveValue(val2Str, operands);

                if (left.compareTo(right) < 0) {
                    result = val1;
                } else {
                    result = val2;
                }
            }
        } 
        // Pattern 2: Multiplication/Division
        else if (expr.contains("*")) {
            String[] parts = expr.split("\\*");
            BigDecimal left = resolveValue(parts[0], operands);
            BigDecimal right = resolveValue(parts[1], operands);
            result = left.multiply(right);
        } else if (expr.contains("/")) {
            String[] parts = expr.split("/");
            BigDecimal left = resolveValue(parts[0], operands);
            BigDecimal right = resolveValue(parts[1], operands);
            result = left.divide(right, 4, RoundingMode.HALF_UP);
        } else {
            result = resolveValue(expr, operands);
        }

        String outputKey = step.getOutputOperands().get("result");
        if (outputKey != null) {
            operands.put(outputKey, result.setScale(2, RoundingMode.HALF_UP));
            // Keep TOTAL_AMOUNT updated if output modifies amount
            if ("TOTAL_AMOUNT".equals(outputKey) || "NET_AMOUNT".equals(outputKey)) {
                operands.put("TOTAL_AMOUNT", result.setScale(2, RoundingMode.HALF_UP));
            }
        }
    }

    private BigDecimal resolveValue(String token, Map<String, Object> operands) {
        if (token.startsWith("operands['") && token.endsWith("']")) {
            String key = token.substring(10, token.length() - 2);
            Object obj = operands.get(key);
            if (obj instanceof BigDecimal) {
                return (BigDecimal) obj;
            } else if (obj instanceof Number) {
                return BigDecimal.valueOf(((Number) obj).doubleValue());
            }
            return BigDecimal.ZERO;
        }
        
        try {
            return new BigDecimal(token);
        } catch (NumberFormatException e) {
            return BigDecimal.ZERO;
        }
    }
}
