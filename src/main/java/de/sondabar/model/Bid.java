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
    List<Feature> features;

    BigDecimal bidPrice;

    public Bid() {
    }

    public Bid(BidResponse bidResponse, Long cid) {
        this.bid = bidResponse.bid;
        this.timestamp = bidResponse.timestamp;

        this.features = new ArrayList<>(bidResponse.features.size());
        this.features.addAll(bidResponse.features.stream().collect(Collectors.toList()));

        this.cid = cid;
        bidResponse.bidLines.stream().filter(bid -> Objects.equals(bid.cid, cid)).forEach(bid -> bidPrice = bid.getPrice());
    }
}
