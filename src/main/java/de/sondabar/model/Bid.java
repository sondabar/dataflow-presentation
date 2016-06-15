package de.sondabar.model;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import org.apache.avro.reflect.Nullable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@DefaultCoder(AvroCoder.class)
public class Bid extends CampaignEvent {

    @Nullable
    protected List<Feature> features;

    protected BigDecimal bidPrice;

    public Bid() {
    }

    public Bid(BidResponse bidResponse, Long cid) {
        bid = bidResponse.bid;
        timestamp = bidResponse.timestamp;

        features = new ArrayList<>(bidResponse.features.size());
        features.addAll(bidResponse.features.stream().collect(Collectors.toList()));

        this.cid = cid;
        bidResponse.bidLines.stream().filter(bid -> Objects.equals(bid.cid, cid)).forEach(bid -> bidPrice = bid.getPrice());
    }
}
