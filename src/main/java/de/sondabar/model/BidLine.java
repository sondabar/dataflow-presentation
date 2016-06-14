package de.sondabar.model;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import org.apache.avro.reflect.Nullable;

import java.math.BigDecimal;
import java.util.Map;

@DefaultCoder(AvroCoder.class)
public class BidLine {

    @Nullable
    Long cid;
    @Nullable
    BigDecimal price;

    public BidLine() {
    }

    public BidLine(Long cid, BigDecimal price) {
        this.cid = cid;
        this.price = price;
    }

    public BidLine(Map<String, Object> map) {
        this.cid = Long.valueOf((Integer) map.get("cid"));
        this.price = new BigDecimal((Double) map.get("bpr"));
    }

    public Long getCid() {
        return cid;
    }

    public BigDecimal getPrice() {
        return price;
    }

    @Override
    public String toString() {
        return "Bid{" +
                "cid=" + cid +
                ", price=" + price +
                '}';
    }
}
