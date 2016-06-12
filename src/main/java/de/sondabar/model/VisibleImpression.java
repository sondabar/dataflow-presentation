package de.sondabar.model;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;


@DefaultCoder(AvroCoder.class)
public class VisibleImpression extends CampaignEvent{
    public VisibleImpression() {
        super();
    }

    public VisibleImpression(Long bid, Long timestamp, Long cid) {
        super(bid, timestamp, cid);
    }

    public VisibleImpression(String bid, String timestamp, String cid) {
        super(bid, timestamp, cid);
    }

    public VisibleImpression(String line) {
        super(line);
    }

    @Override
    public String toString() {
        return "VisibleImpression{" +
                "bid=" + bid +
                ", timestamp=" + timestamp +
                ", cid=" + cid +
                "}";
    }
}
