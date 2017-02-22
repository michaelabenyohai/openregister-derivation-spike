package uk.gov.rsf.util;

import uk.gov.rsf.serialization.RegisterResult;

public class RegisterResultException extends RuntimeException {
    private final RegisterResult registerResult;

    public RegisterResultException(RegisterResult registerResult) {
        super(registerResult.getMessage());
        this.registerResult = registerResult;
    }

    public RegisterResult getRegisterResult() {
        return registerResult;
    }
}
