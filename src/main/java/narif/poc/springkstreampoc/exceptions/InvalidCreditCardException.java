package narif.poc.springkstreampoc.exceptions;

public class InvalidCreditCardException extends OrderProcessorException{
    private static final String MSG = "Invalid Credit Card Details provided.";

    public InvalidCreditCardException() {
        super(MSG);
    }

    public InvalidCreditCardException(String message) {
        super(message);
    }

    public InvalidCreditCardException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidCreditCardException(Throwable cause) {
        super(cause);
    }

    protected InvalidCreditCardException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
