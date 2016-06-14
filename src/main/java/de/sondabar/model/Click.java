package de.sondabar.model;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;


@DefaultCoder(AvroCoder.class)
public class Click extends CampaignEvent {
    public Click() {
        super();
    }

    public Click(String bid, Long timestamp, Long cid) {
        super(bid, timestamp, cid);
    }

    public Click(String bid, String timestamp, String cid) {
        super(bid, timestamp, cid);
    }

    public Click(String line) {
        super(line);
    }

    @Override
    public String toString() {
        return "Click{" +
                "bid=" + bid +
                ", timestamp=" + timestamp +
                ", cid=" + cid +
                "}";
    }
}
