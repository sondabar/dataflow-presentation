package de.sondabar.model;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;

import java.math.BigDecimal;


@DefaultCoder(AvroCoder.class)
public class WonBid extends CampaignEvent {

    private BigDecimal price;

    public WonBid() {
        super();
    }

    public WonBid(String bid, Long timestamp, Long cid, final BigDecimal price) {
        super(bid, timestamp, cid);
        this.price = price;
    }

    public WonBid(String bid, String timestamp, String cid) {
        super(bid, timestamp, cid);
    }

    public WonBid(String line) {
        final String[] fields = line.split(",");
        this.bid = fields[0];
        this.timestamp = Long.parseLong(fields[1]);
        this.cid = Long.parseLong(fields[2]);
        this.price = new BigDecimal(fields[3]);
    }

    public BigDecimal getPrice() {
        return price;
    }

    @Override
    public String toString() {
        return "WonBid{" +
                "bid=" + bid +
                ", timestamp=" + timestamp +
                ", cid=" + cid +
                "}";
    }
}
