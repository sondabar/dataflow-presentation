package de.sondabar.model;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;


@DefaultCoder(AvroCoder.class)
public class Impression extends CampaignEvent {
    public Impression() {
        super();
    }

    public Impression(String bid, Long timestamp, Long cid) {
        super(bid, timestamp, cid);
    }

    public Impression(String bid, String timestamp, String cid) {
        super(bid, timestamp, cid);
    }

    public Impression(String line) {
        super(line);
    }

    @Override
    public String toString() {
        return "Impression{" +
                "bid=" + bid +
                ", timestamp=" + timestamp +
                ", cid=" + cid +
                "}";
    }
}
