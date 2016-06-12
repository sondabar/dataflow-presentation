package de.sondabar.model;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;


@DefaultCoder(AvroCoder.class)
public class Conversion extends CampaignEvent {
    public Conversion() {
        super();
    }

    public Conversion(Long bid, Long timestamp, Long cid) {
        super(bid, timestamp, cid);
    }

    public Conversion(String bid, String timestamp, String cid) {
        super(bid, timestamp, cid);
    }

    public Conversion(String line) {
        super(line);
    }

    @Override
    public String toString() {
        return "Conversion{" +
                "bid=" + bid +
                ", timestamp=" + timestamp +
                ", cid=" + cid +
                "}";
    }
}
