package uk.gov.rsf.util;

public class SerializedRegisterParseException extends RuntimeException {
    public SerializedRegisterParseException(String message) {
        super(message);
    }

    public SerializedRegisterParseException(String message, Exception e){
        super(message, e);
    }
}
