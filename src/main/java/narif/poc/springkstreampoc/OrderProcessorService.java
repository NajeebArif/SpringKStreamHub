package narif.poc.springkstreampoc;

import narif.poc.springkstreampoc.exceptions.InvalidCreditCardException;
import narif.poc.springkstreampoc.model.OrderInputMsg;

public class OrderProcessorService {

    public static OrderInputMsg processOrderMsg(OrderInputMsg value) {
        value.setCreditCardNumber(maskCreditCardInformation(value));
        return value;
    }

    private static String maskCreditCardInformation(OrderInputMsg value) {
        String creditCardNumber = value.getCreditCardNumber();
        String[] split = creditCardNumber.split("-");
        if(split.length != 4){
            throw new InvalidCreditCardException();
        }
        return "XXXX-XXXX-XXXX-"+split[3];
    }
}
