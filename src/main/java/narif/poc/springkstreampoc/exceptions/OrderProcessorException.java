package narif.poc.springkstreampoc.exceptions;

public class OrderProcessorException extends RuntimeException{
    private static final String MSG = "Error while processing Order";
    public OrderProcessorException() {
        super(MSG);
    }

    public OrderProcessorException(String message) {
        super(message);
    }

    public OrderProcessorException(String message, Throwable cause) {
        super(message, cause);
    }

    public OrderProcessorException(Throwable cause) {
        super(cause);
    }

    protected OrderProcessorException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
