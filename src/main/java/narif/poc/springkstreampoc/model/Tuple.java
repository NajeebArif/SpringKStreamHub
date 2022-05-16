package narif.poc.springkstreampoc.model;

import java.util.Optional;

public class Tuple <T, U>{

    private final T t;
    private final U u;


    public Tuple(T t, U u) {
        this.t = t;
        this.u = u;
    }

    public Optional<T> getOptionalT(){
        return Optional.ofNullable(t);
    }

    public Optional<U> getOptionalU(){
        return Optional.ofNullable(u);
    }
}
