package de.sondabar.model;

import org.apache.avro.reflect.Nullable;

import java.math.BigDecimal;

public class Bid {

    @Nullable
    Long cid;
    @Nullable
    BigDecimal price;

    public Bid() {
    }

    public Bid(Long cid, BigDecimal price) {
        this.cid = cid;
        this.price = price;
    }

    public Bid(String line) {

    }
}
