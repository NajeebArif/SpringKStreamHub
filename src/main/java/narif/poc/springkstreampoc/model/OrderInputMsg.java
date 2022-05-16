package narif.poc.springkstreampoc.model;

import lombok.Data;

@Data
public class OrderInputMsg {
    private String orderId;
    private String itemName;
    private String userName;
    private String creditCardNumber;
    private Double orderAmount;
}
