package uk.gov.rsf.util;

public class SerializationFormatValidationException extends RuntimeException {
    public SerializationFormatValidationException(String jsonString) {
        super(jsonString);
    }
}
