package com.evn.billing.mediation.validation;

import java.util.ArrayList;
import java.util.List;

public class ValidationResult {
    private boolean valid = true;
    private String status = "READY_FOR_BILLING";
    private final List<String> errors = new ArrayList<>();

    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public List<String> getErrors() {
        return errors;
    }

    public void addError(String error) {
        this.valid = false;
        this.status = "PENDING_MANUAL";
        this.errors.add(error);
    }
}
