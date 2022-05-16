package narif.poc.springkstreampoc.model;

import lombok.Data;

@Data
public class Tuple <T, U>{

    private final T t;
    private final U u;

}
