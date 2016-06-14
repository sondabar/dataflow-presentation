package de.sondabar.model;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import org.apache.avro.reflect.Nullable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

@DefaultCoder(AvroCoder.class)
public class Result extends CampaignEvent{

    @Nullable
    List<Feature> features;

    public enum State {
        Bid, WonBid, Impression, VisibleImpression, Click, Conversion
    }

    private State state = State.Bid;

    @Nullable
    private BigDecimal bidPrice;

    @Nullable
    private BigDecimal winPrice;

    public Result() {
    }

    public Result(Bid bid) {
        this.bid = bid.bid;
        this.cid = bid.cid;
        this.bidPrice = bid.bidPrice;
        this.timestamp = bid.timestamp;
        this.features = new ArrayList<>(bid.features);
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }
}
