package de.sondabar.model;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import org.apache.avro.reflect.Nullable;

import java.util.List;
import java.util.UUID;

@DefaultCoder(AvroCoder.class)
public class BidRequest extends Event {

    final String id;

    @Nullable
    List<Feature> features;
    @Nullable
    List<Bid> bids;

    public BidRequest() {
        id = UUID.randomUUID().toString();
    }
}
