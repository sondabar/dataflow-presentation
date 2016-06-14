package de.sondabar.model;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import org.apache.avro.reflect.Nullable;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

@DefaultCoder(AvroCoder.class)
public class CampaignResult extends CampaignEvent {

    @Nullable
    private List<Feature> features;

    public enum State {
        Bid, WonBid, Impression, VisibleImpression, Click, Conversion
    }

    private State state = State.Bid;

    @Nullable
    private BigDecimal bidPrice;

    @Nullable
    private BigDecimal winPrice;

    public CampaignResult() {
    }

    public CampaignResult(Bid bid, WonBid wonBid) {
        this.bid = bid.bid;
        this.cid = bid.cid;
        this.bidPrice = bid.bidPrice;
        this.timestamp = bid.timestamp;
        this.features = new ArrayList<>(bid.features);
        if (wonBid != null) {
            winPrice = wonBid.getPrice();
        }
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    @Override
    public String toString() {
        return "CampaignResult{" +
                "features=" + features +
                ", state=" + state +
                ", bidPrice=" + bidPrice +
                ", winPrice=" + winPrice +
                '}';
    }
}
